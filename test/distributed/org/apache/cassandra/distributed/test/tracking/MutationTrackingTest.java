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

package org.apache.cassandra.distributed.test.tracking;

import java.nio.file.Files;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.apache.cassandra.replication.CoordinatorLogId;
import org.apache.cassandra.replication.MutationSummary;
import org.apache.cassandra.replication.Offsets;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.ReplicationType;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.getOnlyLogId;
import static org.apache.cassandra.distributed.test.tracking.MutationTrackingUtils.summaryIdSpace;

public class MutationTrackingTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingTest.class);

    @Test
    public void testBasicWritePath() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true"))
                                      .start())
        {

            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            String keyspaceName = KEYSPACE;
            cluster.get(1).runOnInstance(() -> {

                KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(keyspaceName);
                Assert.assertEquals(ReplicationType.tracked, keyspace.params.replicationType);
            });

            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            cluster.get(1).runOnInstance(() -> {
                TableMetadata table = Schema.instance.getTableMetadata(keyspaceName, "tbl");
                DecoratedKey dk = Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(1));
                MutationSummary summary = MutationTrackingService.instance.createSummaryForKey(dk, table.id, false);
                CoordinatorLogId logId = getOnlyLogId(summary);
                Offsets summaryIds = summaryIdSpace(summary.get(logId));
                Assert.assertEquals(1, summaryIds.offsetCount());
            });
        }
    }

    @Test
    public void testHintsNotWrittenOnFailedWrite() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true")
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {

            String keyspaceName = KEYSPACE;
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));

            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k int primary key, v int);"));

            // block messages to node 3
            cluster.filters().allVerbs().to(3).drop();
            cluster.filters().allVerbs().from(3).drop();
            UUID node3HostId = cluster.get(3).callOnInstance(() -> StorageService.instance.getLocalHostUUID());
            long hints = cluster.get(1).callOnInstance(() -> StorageMetrics.totalHints.getCount());

            // confirm no hints for node 3
            cluster.get(1).runOnInstance(() -> Assert.assertEquals(0, HintsService.instance.getTotalHintsSize(node3HostId)));
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k, v) VALUES (1, 1)"), ConsistencyLevel.QUORUM);

            // wait for write timeout
            Thread.sleep(5000);

            // TODO: confirm hints aren't written
            cluster.get(1).runOnInstance(() -> {
                Assert.assertEquals(hints, StorageMetrics.totalHints.getCount());
            });
        }
    }

    @Test
    public void testTrackedImport() throws Throwable
    {
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                            .with(Feature.GOSSIP)
                                                            .set("mutation_tracking_enabled", "true")
                                                            .set("write_request_timeout", "1000ms"))
                                      .start())
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = " +
                                              "{'class': 'SimpleStrategy', 'replication_factor': 3} " +
                                              "AND replication_type='tracked';"));
            String TABLE = "tbl";
            String KEYSPACE_TABLE = String.format("%s.%s", KEYSPACE, TABLE);
            String schema = String.format(withKeyspace("CREATE TABLE %s." + TABLE + " (k int primary key, v int);"));
            cluster.schemaChange(schema);

            // Hack: need to bounce for KeyspaceShards to be created for new table, schema changes not yet supported
            bounce(cluster);

            // Needs to run outside of instance executor because creates schema
            String file = Files.createTempDirectory(MutationTrackingTest.class.getSimpleName()).toString();

            try (CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                           .forTable(schema)
                                                           .inDirectory(file)
                                                           .using("INSERT INTO " + KEYSPACE_TABLE + " (k, v) " + "VALUES (?, ?)")
                                                           .build())
            {
                writer.addRow(1, 1);
            }

            for (IInvokableInstance instance : cluster)
            {
                logger.info("Checking instance {} empty before import", instance.config().num());
                Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE));
                AssertUtils.assertRows(rows); // empty
            }

            cluster.get(1).runOnInstance(() -> {
                ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(KEYSPACE, TABLE);
                Set<String> paths = Set.of(file);
                logger.info("Importing SSTables {}", paths);
                cfs.importNewSSTables(paths, true, true, true, true, true, true, true);
            });

            for (IInvokableInstance instance : cluster)
            {
                logger.info("Checking propagation of imported SSTable to {}", instance.config().num());
                // SinglePartition + PartitionRange
                {
                    Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE + " WHERE k = 1"));
                    AssertUtils.assertRows(rows, AssertUtils.row(1, 1));
                }
                {
                    Object[][] rows = instance.executeInternal(withKeyspace("SELECT * FROM %s." + TABLE));
                    AssertUtils.assertRows(rows, AssertUtils.row(1, 1));
                }
            }
        }
    }

    private static void bounce(Cluster cluster)
    {
        cluster.forEach(instance -> {
            try
            {
                instance.shutdown().get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new RuntimeException(e);
            }
            instance.startup();
        });
    }
}
