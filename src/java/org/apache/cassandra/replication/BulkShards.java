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

package org.apache.cassandra.replication;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.IntSupplier;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.replication.bulk.RangeTransfer;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;

/**
 *
 */
public class BulkShards
{
    Collection<BulkShard> shards;

    private BulkShards(Collection<BulkShard> shards)
    {
        this.shards = shards;
    }

    public static BulkShards create(KeyspaceMetadata keyspace, ClusterMetadata cluster, IntSupplier logIdProvider)
    {
        List<BulkShard> shards = new ArrayList<>();

        // This node could coordinate imports for any replica set, so ensure it's always considered a participant, even
        // if it doesn't own the range
        cluster.placements.get(keyspace.params.replication).writes.forEach((tokenRange, forRange) -> {
            boolean containsSelf = forRange.get().containsSelf();
            int size = containsSelf ? forRange.size() : forRange.size() + 1;
            IntArrayList participants = new IntArrayList(size, IntArrayList.DEFAULT_NULL_VALUE);
            for (InetAddressAndPort endpoint : forRange.endpoints())
                participants.add(cluster.directory.peerId(endpoint).id());
            if (!containsSelf)
                participants.add(cluster.myNodeId().id());

            BulkShard shard = new BulkShard(keyspace.name, cluster.myNodeId().id(), new Participants(participants), forRange.lastModified(), logIdProvider);
            shards.add(shard);
        });

        return new BulkShards(shards);
    }

    public BulkShard register(RangeTransfer rangeTransfer)
    {

    }
}
