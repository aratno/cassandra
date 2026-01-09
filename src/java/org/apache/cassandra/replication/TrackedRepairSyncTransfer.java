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

import java.util.Collection;
import java.util.List;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.repair.SyncStat;
import org.apache.cassandra.repair.SyncTask;

/**
 * Repair sync tasks (that stream SSTable contents) must be integrated with Mutation Tracking's bulk transfer handling
 * because any data that is not present on all instances must be expressed as unreconciled in the log, since read
 * reconciliations depend on the log state to guarantee monotonicity of subsequent reads. Streaming sessions for full
 * repair can complete on some instances before others, so we need to represent those completed sessions as unreconciled
 * in the log.
 * <p>
 * TODO: call LocalTransfers.instance().save(this) once the RepairJob starts streaming
 */
public class TrackedRepairSyncTransfer extends AbstractCoordinatedBulkTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedRepairSyncTransfer.class);

    public TrackedRepairSyncTransfer(ShortMutationId id, Collection<SyncTask> tasks)
    {
        super(id);
        for (SyncTask task : tasks)
            streamResults.put(task.nodePair().peer, SingleTransferResult.Init());
    }

    /**
     * When all {@link SyncTask} for a repair have completed, follow the bulk transfer activation path to safely make
     * the new data live and tracked in the log. This needs to include all replicas of {@link #id()}, even those that
     * did not receive anything as part of the repair, otherwise any read reconciliation will fail to complete.
     */
    public void activate(List<SyncStat> syncs)
    {
        logger.debug("{} Activating {}: {}", logPrefix(), this, syncs);

        for (SyncStat sync : syncs)
        {
            Preconditions.checkNotNull(sync.planId);
            streamResults.put(sync.nodes.peer, SingleTransferResult.StreamComplete(sync.planId));
        }

        // Need to activate on all replicas, not just ones with SyncStats
        // TODO: How can I get the replicas for this ID? And I need to send them a TransferActivation with no planId,
        // so they add the transferId to their log.
        activate(streamResults.keySet());
    }

    @Override
    public String toString()
    {
        return "TrackedRepairSyncTransfer{" +
               "streamResults=" + streamResults +
               '}';
    }
}
