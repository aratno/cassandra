/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.distributed.test;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesNoArguments;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class TrackedKeyspaceRepairSupportTest extends TestBaseImpl
{
    private static final String KEYSPACE = "tracked_ks";
    private static final String TABLE = "tbl";
    private static final String KEYSPACE_TABLE = String.format("%s.%s", KEYSPACE, TABLE);

    @Test
    public void testIncrementalRepairRejected() throws IOException
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true"))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';");

            NodeToolResult result = cluster.get(1).nodetoolResult("repair", KEYSPACE);
            result.asserts().failure();
            Assertions.assertThat(result.getError()).hasMessageMatching("Tracked keyspaces do not support incremental repair");
        }
    }

    @Test
    public void testValidationRepairRejected() throws IOException
    {
        try (Cluster cluster = Cluster.build(3)
                                     .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                           .with(Feature.GOSSIP)
                                                           .set("mutation_tracking_enabled", "true"))
                                     .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';");

            NodeToolResult result = cluster.get(1).nodetoolResult("repair", "--validate", KEYSPACE);
            result.asserts().failure();
            Assertions.assertThat(result.getError()).hasMessageMatching("Tracked keyspaces do not support validation repair");
        }
    }

    @Test
    public void testFullRepairPermitted() throws IOException
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true"))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';");
            NodeToolResult result = cluster.get(1).nodetoolResult("repair", "--full", KEYSPACE);
            result.asserts().success();
        }
    }

    @Test
    public void testFullRepairPartiallyCompleteAnomaly() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        try (Cluster cluster = Cluster.build(3)
                                     .withInstanceInitializer(RepairFailureHelper::install)
                                     .withConfig(cfg -> cfg
                                                           .with(Feature.NETWORK)
                                                           .with(Feature.GOSSIP)
                                                           .set("mutation_tracking_enabled", "true"))
                                     .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';");
            String TABLE_SCHEMA_CQL = "CREATE TABLE " + KEYSPACE + '.' + TABLE + " (k INT PRIMARY KEY, v INT)";
            cluster.schemaChange(TABLE_SCHEMA_CQL);

            IInvokableInstance COORDINATING = cluster.get(1);
            IInvokableInstance RECEIVING = cluster.get(2);
            IInvokableInstance MISSING = cluster.get(3);

            /*
            If we were to start this process with a normal write, that write would be added to the log. When repair
            validation runs and finds the mismatching range, it streams the log. MISSING then receives the log and
            applies the new mutation to the memtable, where it's visible to reads.

            We want to emulate a situation where one node has a mutation that's not present in the log, hence the
            roundabout write path. Once the mutation has completed and is repaired on the coordinator, it's been
            (durably) reconciled on all replicas. Then, drop this SSTable on the other peers and we should have a full
            repair digest mismatch.
            */
            COORDINATING.coordinator().execute("INSERT INTO " + KEYSPACE_TABLE + " (k, v) " + "VALUES (?, ?)", ALL, 1, 1);
            COORDINATING.flush(KEYSPACE);
            Awaitility.waitAtMost(1, TimeUnit.MINUTES).pollDelay(1, TimeUnit.SECONDS)
                      .until(() -> {
                          boolean isRepaired = COORDINATING.callOnInstance(() -> {
                              ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                              Set<SSTableReader> sstables = cfs.getLiveSSTables();
                              if (sstables.size() != 1)
                                  return false;
                              SSTableReader sstable = sstables.iterator().next();
                              return sstable.isRepaired();
                          });
                          if (!isRepaired)
                              COORDINATING.forceCompact(KEYSPACE, TABLE);
                          return isRepaired;
                      });
            List.of(RECEIVING, MISSING).forEach(instance -> instance.runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                cfs.truncateBlockingWithoutSnapshot();
            }));
            cluster.forEach(instance -> {
                // Before repair, peers should have no data
                Object[][] rows = instance.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE k = 1");
                if (instance == COORDINATING)
                    AssertUtils.assertRows(rows, row(1, 1));
                else
                    AssertUtils.assertRows(rows);
            });

            // Prevent repair stream from completing
            MISSING.runOnInstance(() -> RepairFailureHelper.shouldWait.set(true));

            // Run full repair from COORDINATING
            ExecutorService repairExecutor = Executors.newSingleThreadExecutor();
            Future<NodeToolResult> repairing = repairExecutor.submit(() -> COORDINATING.nodetoolResult("repair", "--full", KEYSPACE));

            Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS)
                      .until(() -> {
                          int finished = RepairFailureHelper.getFinishedRepairs(RECEIVING);
                          return finished > 0;
                      });
            MISSING.runOnInstance(() -> RepairFailureHelper.shouldThrow.set(true));
            repairing.get(10, TimeUnit.SECONDS);
            repairExecutor.shutdown();

            // RECEIVING should have the repaired SSTable but not MISSING
            RECEIVING.runOnInstance(() -> {
                Set<SSTableReader> live = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE).getTracker().getView().liveSSTables();
                Assertions.assertThat(live).isNotEmpty();
            });
            MISSING.runOnInstance(() -> {
                Set<SSTableReader> live = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE).getTracker().getView().liveSSTables();
                Assertions.assertThat(live).isEmpty();
            });

            Object[][] rows = RECEIVING.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE k = 1");
            AssertUtils.assertRows(rows, row(1, 1));

            Object[][] empty = MISSING.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE k = 1");
            AssertUtils.assertRows(empty);

            /*
            This test will fail periodically because the commitlog has shut down but LogStatePersister wants to update
            the system table.
            */
        }
    }

    public static class RepairFailureHelper
    {
        private static final Logger logger = LoggerFactory.getLogger(RepairFailureHelper.class);

        static AtomicBoolean shouldThrow = new AtomicBoolean(false);
        static AtomicBoolean shouldWait = new AtomicBoolean(false);
        static AtomicInteger count = new AtomicInteger(0);

        /**
         * {@link org.apache.cassandra.db.streaming.CassandraStreamReceiver#finished}
         */
        public static void install(ClassLoader classLoader, Integer instanceNum)
        {
            new ByteBuddy().rebase(org.apache.cassandra.db.streaming.CassandraStreamReceiver.class)
                           .method(named("finished").and(takesNoArguments()))
                           .intercept(to(RepairFailureHelper.class))
                           .make()
                           .load(classLoader, ClassLoadingStrategy.Default.INJECTION);
        }

        public static void run(@SuperCall Callable<Void> zuper) throws Exception
        {
            while (shouldWait.get())
            {
                if (shouldThrow.get())
                    throw new RuntimeException("Test: failing stream session");

                logger.info("Test: blocking finish of stream session");
                Thread.sleep(1_000);
            }
            zuper.call();
            logger.info("Test: finished stream session");
            count.incrementAndGet();
        }

        private static int getFinishedRepairs(IInvokableInstance instance)
        {
            return instance.callOnInstance(() -> RepairFailureHelper.count.get());
        }
    }
}