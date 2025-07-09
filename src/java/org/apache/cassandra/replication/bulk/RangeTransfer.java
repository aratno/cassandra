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

package org.apache.cassandra.replication.bulk;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ownership.VersionedEndpoints;
import org.apache.cassandra.utils.Interval;

/**
 * A transfer for a single replica set.
 */
public class RangeTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(RangeTransfer.class);

    private final TableId tableId;
    private final Collection<SSTableReader> sstables;
    private volatile Phase phase;

    RangeTransfer(TableId tableId, Collection<SSTableReader> sstables)
    {
        this.tableId = tableId;
        this.sstables = sstables;
    }

    private enum Phase
    {
        INIT,
        FAILED;
    }

    private void transition(Phase phase)
    {
        this.phase = phase;
    }

    void send(Keyspace keyspace)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        StreamPlan plan = new StreamPlan(StreamOperation.IMPORT);

        // No need to flush, only using non-live SSTables already on disk
        plan.flushBeforeTransfer(false);

        SSTableIntervalTree intervals = SSTableIntervalTree.buildSSTableIntervalTree(sstables);
        ImmutableList<VersionedEndpoints.ForRange> endpoints = cm.placements.get(keyspace.getMetadata().params.replication).writes.endpoints;
        for (VersionedEndpoints.ForRange forRange : endpoints)
        {
            Range<Token> range = forRange.range();
            List<SSTableReader> sstablesForEndpoint = intervals.search(Interval.create(range.left.minKeyBound(), range.right.maxKeyBound()));

            /* REVIEW NOTES
            Right now for simplicity, streaming from coordinator to itself instead of copying files. This has some
            perks: (1) it allows us to import out-of-range SSTables using the same paths, and (2) it uses the
            existing lifecycle management to handle crash-safety, so don't need to deal with atomic multi-file copy.
            */

            for (SSTableReader sstable : sstablesForEndpoint)
            {
                List<SSTableReader.PartitionPositionBounds> positions = sstable.getPositionsForRanges(Collections.singleton(range));
                List<Range<Token>> ranges = Collections.singletonList(range);
                long estimatedKeys = sstable.estimatedKeysForRanges(ranges);
                for (Replica replica : forRange.get())
                {
                    OutgoingStream stream = new CassandraOutgoingFile(StreamOperation.IMPORT, sstable.ref(), positions, ranges, estimatedKeys);
                    plan.transferStreams(replica.endpoint(), Collections.singleton(stream));
                }
            }
        }

        logger.info("Executing bulk transfer {}", plan.planId());
        return new Sending(this, plan.execute());
    }

    void onReconciled()
    {
        ColumnFamilyStore cfs = ColumnFamilyStore.getIfExists(this.tableId);
        if (cfs == null)
            throw new RuntimeException("Unknown table " + cfs);
        cfs.addSSTables(sstables);
    }
}
