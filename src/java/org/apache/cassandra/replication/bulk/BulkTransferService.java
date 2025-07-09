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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.IntSupplier;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.replication.BulkShards;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;

public class BulkTransferService
{
    private static final Logger logger = LoggerFactory.getLogger(BulkTransferService.class);

    private final ConcurrentMap<String, BulkShards> shards = new ConcurrentHashMap<>();

    public void init(KeyspaceMetadata keyspace, ClusterMetadata metadata, IntSupplier nextHostLogId)
    {
        BulkShards existing = shards.putIfAbsent(keyspace.name, BulkShards.create(keyspace, metadata, nextHostLogId));
        Preconditions.checkState(existing == null);
    }

    public void start(String keyspace, TableId table, Collection<SSTableReader> sstables)
    {
        BulkShards bulkShards = shards.get(keyspace);
        Preconditions.checkState(bulkShards != null);

        RangeTransfers transfers = RangeTransfers.create(keyspace, table, sstables);
        bulkShards.register(transfer);

        // transfer.send(Keyspace.open(keyspace));
    }

    /**
     *
     */
    public void streamFinished(TableId tableId, MutationId transferId, Collection<SSTableReader> readers)
    {

    }
}
