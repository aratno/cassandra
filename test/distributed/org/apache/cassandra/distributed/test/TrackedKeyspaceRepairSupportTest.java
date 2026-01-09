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
import java.nio.ByteBuffer;
import java.util.ArrayList;
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
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.streaming.CassandraStreamReceiver;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.tracking.BulkTransfersTest;
import org.apache.cassandra.distributed.test.tracking.MutationTrackingReadReconciliationTest;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.replication.ActiveLogReconciler;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.TransferActivation;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;

import static net.bytebuddy.implementation.MethodDelegation.to;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesNoArguments;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class TrackedKeyspaceRepairSupportTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedKeyspaceRepairSupportTest.class);

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
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k INT PRIMARY KEY, v INT)");

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
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k INT PRIMARY KEY, v INT)");

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
                                     .withInstanceInitializer(StreamReceiverFailureHelper::install)
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

            The idea here is to emulate logical data corruption, where SSTables mismatch but the logs are in agreement.
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
            MISSING.runOnInstance(() -> StreamReceiverFailureHelper.shouldWait.set(true));

            // Run full repair from COORDINATING
            {
                ExecutorService repairExecutor = Executors.newSingleThreadExecutor();
                Future<NodeToolResult> repair = repairExecutor.submit(() -> COORDINATING.nodetoolResult("repair", "--full", KEYSPACE));
                Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS)
                          .until(() -> {
                              int finished = StreamReceiverFailureHelper.getFinishedRepairs(RECEIVING);
                              return finished > 0;
                          });
                MISSING.runOnInstance(() -> StreamReceiverFailureHelper.shouldThrow.set(true));
                repair.get(10, TimeUnit.SECONDS).asserts().failure();
                repairExecutor.shutdown();
            }

            // Even after partial repair, RECEIVED should not move its SSTable to the live set, since the repair failed
            cluster.forEach(instance -> {
                Object[][] rows = instance.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE k = 1");
                if (instance == COORDINATING)
                    AssertUtils.assertRows(rows, row(1, 1));
                else
                    AssertUtils.assertRows(rows);
            });

            /*
            At this point, the repair is complete and partially applied. RECEIVED has an SSTable it received from
            repair, and MISSING has no SSTables. If we were to do a tracked data read against RECEIVED, we'd have an
            emptpy summary but rows, and if we were to execute the same read against MISSING we'd have an entirely
            empty response. This would break monotonicity if a client executes a QUORUM read against RECEIVED then
            against missing, because the empty summaries lead to no reconciliation happening.

            To provide monotonicity in this scenario, we integrate the full repair with bulk transfer machinery and
            tag the SSTables with transfer IDs that can be included in summaries and reconciled. Then, the initial data
            read against RECEIVED includes transfer IDs that are reconciled. Reconciliation detects that RECEIVED has an
            SSTable that isn't present on MISSING and streams them, so the subsequent read against MISSING is up to
            date.
            */

            {
                MISSING.runOnInstance(() -> StreamReceiverFailureHelper.shouldWait.set(false));
                MISSING.runOnInstance(() -> StreamReceiverFailureHelper.shouldThrow.set(false));
                // Don't let coordinating act as a replica for the read
                cluster.filters().inbound().to(ClusterUtils.instanceId(COORDINATING)).drop();
                MutationTrackingReadReconciliationTest.awaitNodeDead(RECEIVING, COORDINATING);
            }
            // Repair did not succeed sync, so it did not proceed to activation, so it's not visible on RECEIVING.
            {
                Object[][] rows = RECEIVING.coordinator().execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE k = 1", QUORUM);
                AssertUtils.assertRows(rows); // empty
            }
            cluster.filters().reset();
            MutationTrackingReadReconciliationTest.awaitNodeAlive(RECEIVING, COORDINATING);

            // Another repair succeeds, all peers should now agree on the local data
            long mark = COORDINATING.logs().mark();
            COORDINATING.nodetoolResult("repair", "--full", KEYSPACE).asserts().success();
            List<String> logs = COORDINATING.logs().grep(mark, "Activating .* on ").getResult();
            Assertions.assertThat(logs).isNotEmpty();
            cluster.forEach(instance -> {
                Object[][] rows = instance.executeInternal("SELECT * FROM " + KEYSPACE + ".tbl WHERE k = 1");
                AssertUtils.assertRows(rows, row(1, 1));
            });

            /*
            This test will fail periodically because the commitlog has shut down but LogStatePersister wants to update
            the system table.
            */
        }
    }

    public static class StreamReceiverFailureHelper
    {
        private static final Logger logger = LoggerFactory.getLogger(StreamReceiverFailureHelper.class);

        static AtomicBoolean shouldThrow = new AtomicBoolean(false);
        static AtomicBoolean shouldWait = new AtomicBoolean(false);
        static AtomicInteger count = new AtomicInteger(0);

        /**
         * {@link CassandraStreamReceiver#finished}
         */
        public static void install(ClassLoader classLoader, Integer instanceNum)
        {
            new ByteBuddy().rebase(CassandraStreamReceiver.class)
                           .method(named("finished").and(takesNoArguments()))
                           .intercept(to(StreamReceiverFailureHelper.class))
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
            return instance.callOnInstance(() -> StreamReceiverFailureHelper.count.get());
        }
    }

    @Test
    public void testFullRepairCleanupOnFailure() throws IOException, ExecutionException, InterruptedException, TimeoutException
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withInstanceInitializer(StreamReceiverFailureHelper::install)
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

            // Write a single row to COORDINATING node only
            COORDINATING.executeInternal("INSERT INTO " + KEYSPACE_TABLE + " (k, v) VALUES (?, ?)", 1, 100);

            // Before repair, only COORDINATING has data
            for (IInvokableInstance instance : cluster)
            {
                Object[][] rows = instance.executeInternal("SELECT * FROM " + KEYSPACE_TABLE + " WHERE k = 1");
                if (instance == COORDINATING)
                    AssertUtils.assertRows(rows, row(1, 100));
                else
                    AssertUtils.assertRows(rows); // empty
            }

            // Prevent repair stream from completing
            MISSING.runOnInstance(() -> StreamReceiverFailureHelper.shouldWait.set(true));

            // Run full repair from COORDINATING
            {
                ExecutorService repairExecutor = Executors.newSingleThreadExecutor();
                Future<NodeToolResult> repair = repairExecutor.submit(() -> COORDINATING.nodetoolResult("repair", "--full", KEYSPACE));
                Awaitility.waitAtMost(10, TimeUnit.SECONDS).pollDelay(1, TimeUnit.SECONDS)
                          .until(() -> {
                              int finished = StreamReceiverFailureHelper.getFinishedRepairs(RECEIVING);
                              return finished > 0;
                          });

                // Repair completed against RECEIVING, so it should have pending SSTables
                {
                    List<String> pending = getPendingSSTablePaths(RECEIVING);
                    Assertions.assertThat(pending).isNotEmpty();
                }

                MISSING.runOnInstance(() -> StreamReceiverFailureHelper.shouldThrow.set(true));
                repair.get(10, TimeUnit.SECONDS).asserts().failure();
                repairExecutor.shutdown();
            }

            // No pending SSTables on COORDINATING because they're streamed from the live set
            // No pending SSTables on MISSING because of stream failure injection
            // No pending SSTables on RECEIVING because they were cleaned up when the repair failed
            for (IInvokableInstance instance : cluster)
            {
                Object[][] rows = instance.executeInternal("SELECT * FROM " + KEYSPACE_TABLE + " WHERE k = 1");
                if (instance == COORDINATING)
                    AssertUtils.assertRows(rows, row(1, 100));
                else
                    AssertUtils.assertRows(rows); // empty

                List<String> pending = getPendingSSTablePaths(instance);
                Assertions.assertThat(pending).isEmpty();
            }
        }
    }

    private static List<String> getPendingSSTablePaths(IInvokableInstance instance)
    {
        return instance.callOnInstance(() -> {
            ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
            Set<File> pendingLocations = cfs.getDirectories().getPendingLocations();

            List<String> pendingUuidDirs = new ArrayList<>();
            for (File pendingDir : pendingLocations)
            {
                File[] uuidDirs = pendingDir.listUnchecked(File::isDirectory);
                for (File dir : uuidDirs)
                    pendingUuidDirs.add(dir.absolutePath());
            }
            return pendingUuidDirs;
        });
    }

    // This should be aligned to a single shard: (-3074457345618258603,3074457345618258601]
    private final static long TOKEN_VALUE = 1;
    private final static Token TOKEN = new Murmur3Partitioner.LongToken(TOKEN_VALUE);
    private final static ByteBuffer KEY = Murmur3Partitioner.LongToken.keyForToken(TOKEN.getLongValue());
    private final static Range<Token> SHARD_ALIGNED_RANGE = new Range<>(new Murmur3Partitioner.LongToken(TOKEN_VALUE - 10), new Murmur3Partitioner.LongToken(TOKEN_VALUE + 10));
    static
    {
        DecoratedKey reversed = Murmur3Partitioner.instance.decorateKey(TrackedKeyspaceRepairSupportTest.KEY);
        Assertions.assertThat(reversed.getToken()).isEqualTo(TOKEN);
    }

    @Test
    public void testFullRepairShardAlignedRangeHappyPath() throws IOException
    {
        testFullRepair("repair", "--start-token", SHARD_ALIGNED_RANGE.left.toString(), "--end-token", SHARD_ALIGNED_RANGE.right.toString(), "--full", KEYSPACE);
    }

    @Test
    public void testFullRepairAcrossShardsHappyPath() throws IOException
    {
        testFullRepair("repair", "--full", KEYSPACE);
    }

    public void testFullRepair(String... repairCommandAndArgs) throws IOException
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true"))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (pk BLOB PRIMARY KEY, v INT)");

            IInvokableInstance coordinator = cluster.get(1);
            coordinator.executeInternal("INSERT INTO " + KEYSPACE_TABLE + " (pk, v) VALUES (?, 1)", KEY);

            // Write should only be present on instance 1
            cluster.forEach(instance -> {
                Object[][] rows = instance.executeInternal("SELECT * FROM " + KEYSPACE_TABLE + " WHERE pk = ?", KEY);
                if (ClusterUtils.instanceId(instance) == 1)
                    AssertUtils.assertRows(rows, row(KEY, 1));
                else
                    AssertUtils.assertRows(rows); // empty
            });

            long mark = coordinator.logs().mark();
            NodeToolResult result = coordinator.nodetoolResult(repairCommandAndArgs);
            result.asserts().success();
            List<String> logs = coordinator.logs().grep(mark, "Created 2 sync tasks based on 3 merkle tree responses").getResult();
            Assertions.assertThat(logs).isNotEmpty();

            // Write visible on all instances after repair
            cluster.forEach(instance -> {
                Object[][] rows = instance.executeInternal("SELECT * FROM " + KEYSPACE_TABLE + " WHERE pk = ?", KEY);
                AssertUtils.assertRows(rows, row(KEY, 1));
            });
        }
    }

    @Test
    public void testRepairFailsOnMissedActivation() throws IOException
    {
        int MISSED_ACTIVATION = 3;
        try (Cluster cluster = Cluster.build(3)
                                      .withInstanceInitializer(BulkTransfersTest.ByteBuddyInjections.SkipActivation.install(MISSED_ACTIVATION))
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true")
                                                            .set("write_request_timeout", "1000ms")
                                                            .set("repair_request_timeout", "2s")
                                                            .set("stream_transfer_task_timeout", "10s"))
                                      .start())
        {
            cluster.schemaChange("CREATE KEYSPACE " + KEYSPACE + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3} AND replication_type='tracked';");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE_TABLE + " (k INT PRIMARY KEY, v INT)");

            IInvokableInstance COORDINATING = cluster.get(1);
            IInvokableInstance MISSING = cluster.get(MISSED_ACTIVATION);

            COORDINATING.executeInternal("INSERT INTO " + KEYSPACE_TABLE + " (k, v) VALUES (?, ?)", 1, 100);

            // Before repair, only instance 1 has data
            for (IInvokableInstance instance : cluster)
            {
                Object[][] rows = instance.executeInternal("SELECT * FROM " + KEYSPACE_TABLE + " WHERE k = 1");
                if (instance == COORDINATING)
                    AssertUtils.assertRows(rows, row(1, 100));
                else
                    AssertUtils.assertRows(rows); // empty
            }

            // Repair fails because MISSING blocked activation
            BulkTransfersTest.ByteBuddyInjections.SkipActivation.setup(cluster, TransferActivation.Phase.COMMIT, true);
            {
                NodeToolResult repair = COORDINATING.nodetoolResult("repair", "--full", KEYSPACE);
                repair.asserts().failure();
            }
            for (IInvokableInstance instance : cluster)
            {
                Object[][] rows = instance.executeInternal("SELECT * FROM " + KEYSPACE_TABLE + " WHERE k = 1");
                List<String> pending = getPendingSSTablePaths(instance);
                if (instance == MISSING)
                {
                    AssertUtils.assertRows(rows); // empty
                    Assertions.assertThat(pending).isNotEmpty();
                }
                else
                {
                    AssertUtils.assertRows(rows, row(1, 100));
                    Assertions.assertThat(pending).isEmpty();
                }
            }

            // Re-enable activation; read reconciliation at ALL should activate the pending SSTables on MISSING
            BulkTransfersTest.ByteBuddyInjections.SkipActivation.setup(cluster, null);
            COORDINATING.coordinator().execute("SELECT * FROM " + KEYSPACE_TABLE + " WHERE k = 1", ALL);
            for (IInvokableInstance instance : cluster)
            {
                Object[][] rows = instance.executeInternal("SELECT * FROM " + KEYSPACE_TABLE + " WHERE k = 1");
                List<String> pending = getPendingSSTablePaths(instance);
                AssertUtils.assertRows(rows, row(1, 100));
                Assertions.assertThat(pending).isEmpty();
            }
        }
    }
}