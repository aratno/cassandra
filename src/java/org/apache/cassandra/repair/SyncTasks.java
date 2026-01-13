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

package org.apache.cassandra.repair;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import com.google.common.collect.Iterators;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.replication.MutationId;
import org.apache.cassandra.replication.MutationTrackingService;
import org.apache.cassandra.replication.Participants;
import org.apache.cassandra.replication.ShortMutationId;

public class SyncTasks extends AbstractCollection<SyncTask>
{
    private final Map<ShortMutationId, Entry> tasks = new HashMap<>();

    public static class Entry
    {
        public Participants participants;
        public Collection<SyncTask> tasks;

        private Entry(Participants participants, Collection<SyncTask> tasks)
        {
            this.participants = participants;
            this.tasks = tasks;
        }
    }

    static SyncTasks untracked(Collection<SyncTask> tasks)
    {
        SyncTasks syncTasks = new SyncTasks();
        syncTasks.tasks.put(MutationId.none(), new Entry(null, tasks));
        return syncTasks;
    }

    /**
     * Mutation Tracking manages tracking metadata within shards that are each responsible for a piece of the owned
     * token space. Executing a full repair across an entire node's ownership will span multiple shards, so repair sync
     * tasks need to be split to each align within a single tracking shard.
     */
    static SyncTasks alignedToShardBoundaries(RepairJobDesc desc, List<SyncTask> tasks)
    {
        Keyspace keyspace = Keyspace.open(desc.keyspace);
        if (keyspace == null || !keyspace.getMetadata().params.replicationType.isTracked())
        {
            return untracked(tasks);
        }

        SyncTasks into = new SyncTasks();
        MutationTrackingService.instance.alignToShardBoundaries(desc.keyspace, tasks, into);
        return into;
    }

    public void addAll(ShortMutationId id, Participants participants, Collection<SyncTask> syncTasks)
    {
        tasks.computeIfAbsent(id, key -> new Entry(participants, new HashSet<>())).tasks.addAll(syncTasks);
    }

    public void forEach(BiConsumer<ShortMutationId, Entry> consumer)
    {
        tasks.forEach(consumer);
    }

    @Override
    public Iterator<SyncTask> iterator()
    {
        Iterator<Entry> entries = tasks.values().iterator();
        Iterator<Iterator<SyncTask>> tasks = Iterators.transform(entries,
            entry -> entry == null ? Collections.emptyIterator() : entry.tasks.iterator());
        return Iterators.concat(tasks);
    }

    @Override
    public int size()
    {
        int sum = 0;
        for (Entry entry : tasks.values())
            sum += entry.tasks.size();
        return sum;
    }
}
