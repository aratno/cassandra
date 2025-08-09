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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.TimeUUID;

/**
 * TODO: Make changes to pending set durable with SystemKeyspace.savePendingLocalTransfer(transfer)?
 * TODO: GC
 */
class PendingLocalTransfers
{
    private static final Logger logger = LoggerFactory.getLogger(PendingLocalTransfers.class);

    private enum State
    {
        PENDING,
        ACTIVATED
    }

    private static class Entry
    {
        private final PendingLocalTransfer transfer;
        private volatile State state = State.PENDING;
        private volatile MutationId transferId = null;

        private Entry(PendingLocalTransfer transfer)
        {
            this.transfer = transfer;
        }
    }

    private final ConcurrentMap<TimeUUID, Entry> transfers = new ConcurrentHashMap<>();

    void markPending(PendingLocalTransfer transfer)
    {
        logger.debug("markPending: {}", transfer);
        Preconditions.checkState(!transfer.sstables.isEmpty());
        Entry existing = transfers.put(transfer.planId, new Entry(transfer));
        Preconditions.checkState(existing == null);
    }

    void markActivating(CoordinatedTransfer transfer, MutationId transferId)
    {
        logger.debug("markActivating: {} {}", transfer, transferId);
        Entry entry = transfers.get(transfer.planId);
        // This can fail due to a stream completing immediately, if it has no sessions
        // Then we never reach the registration of the pending transfer in CassandraStreamReceiver
        // Still need to figure out why stream is empty, maybe SSTable has no data for that range?
        // Preconditions.checkNotNull(entry);
        // Preconditions.checkState(entry.state == State.PENDING);
        // Preconditions.checkState(entry.transferId == null);

        transfer.setTransferId(transferId);
    }

    public void markActivated(TimeUUID planId, MutationId transferId)
    {
        logger.debug("markActivated: {} {}", planId, transferId);
        Entry entry = transfers.get(planId);
        Preconditions.checkState(entry.state == State.PENDING);
        Preconditions.checkState(entry.transferId == null);

        entry.transferId = transferId;
        entry.state = State.ACTIVATED;
    }

    PendingLocalTransfer getPending(TimeUUID planId)
    {
        logger.debug("getPending: {}", planId);
        Entry entry = transfers.get(planId);
        // TEMPORARY: Need to fix bug causing empty streams
        // See comment in PendingLocalTransfers.markActivating
        if (entry == null)
            return null;
        Preconditions.checkNotNull(entry);
        Preconditions.checkState(entry.state == State.PENDING);
        Preconditions.checkState(entry.transferId == null);
        return entry.transfer;
    }

    // TODO: Build indexed view for access, don't scan
    Collection<TransferActivation> getActivated(long logId)
    {
        logger.debug("getActivated: {}", logId);
        List<TransferActivation> activated = new ArrayList<>();
        for (Entry entry : transfers.values())
        {
            if (entry.state == State.ACTIVATED
                && entry.transferId != null && entry.transferId.logId() == logId)
            {
                activated.add(new TransferActivation(entry.transfer.planId, entry.transferId));
            }
        }

        return activated;
    }
}
