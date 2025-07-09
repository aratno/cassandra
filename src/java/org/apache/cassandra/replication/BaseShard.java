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
import java.util.List;
import java.util.function.IntSupplier;

import com.google.common.base.Preconditions;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.jctools.maps.NonBlockingHashMapLong;

/**
 *
 */
public abstract class BaseShard
{
    protected final String keyspace;
    protected final int localHostId;
    protected final Participants participants;
    protected final Epoch sinceEpoch;
    protected final NonBlockingHashMapLong<CoordinatorLog> logs;
    // TODO (expected): add support for log rotation
    protected final CoordinatorLog.CoordinatorLogPrimary currentLocalLog;

    BaseShard(String keyspace, int localHostId, Participants participants, Epoch sinceEpoch, IntSupplier logIdProvider)
    {
        Preconditions.checkArgument(participants.contains(localHostId));

        this.keyspace = keyspace;
        this.localHostId = localHostId;
        this.participants = participants;
        this.sinceEpoch = sinceEpoch;
        this.logs = new NonBlockingHashMapLong<>();
        this.currentLocalLog = startNewLog(localHostId, logIdProvider.getAsInt(), participants);
        CoordinatorLogId logId = currentLocalLog.logId;
        Preconditions.checkArgument(!logId.isNone());
        logs.put(logId.asLong(), currentLocalLog);
    }

    MutationId nextId()
    {
        return currentLocalLog.nextId();
    }

    List<InetAddressAndPort> remoteReplicas()
    {
        List<InetAddressAndPort> replicas = new ArrayList<>(participants.size() - 1);
        for (int i = 0, size = participants.size(); i < size; ++i)
        {
            int hostId = participants.get(i);
            if (hostId != localHostId)
                replicas.add(ClusterMetadata.current().directory.endpoint(new NodeId(hostId)));
        }
        return replicas;
    }

    /**
     * Creates a new coordinator log for this host. Primarily on Shard init (node startup or topology change).
     * Also on keyspace creation.
     */
    protected static CoordinatorLog.CoordinatorLogPrimary startNewLog(int localHostId, int hostLogId, Participants participants)
    {
        CoordinatorLogId logId = new CoordinatorLogId(localHostId, hostLogId);
        return new CoordinatorLog.CoordinatorLogPrimary(localHostId, logId, participants);
    }

    protected CoordinatorLog getOrCreate(MutationId mutationId)
    {
        Preconditions.checkArgument(!mutationId.isNone());
        return getOrCreate(mutationId.logId());
    }

    protected CoordinatorLog getOrCreate(CoordinatorLogId logId)
    {
        return getOrCreate(logId.asLong());
    }

    protected CoordinatorLog getOrCreate(long logId)
    {
        CoordinatorLog log = logs.get(logId);
        return log != null
               ? log : logs.computeIfAbsent(logId, ignore -> CoordinatorLog.create(localHostId, new CoordinatorLogId(logId), participants));
    }
}
