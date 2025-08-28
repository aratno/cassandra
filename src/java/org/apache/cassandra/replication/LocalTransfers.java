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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Stores coordinated and received transfers.
 *
 * TODO: Make changes to pending set durable with SystemKeyspace.savePendingLocalTransfer(transfer)?
 * TODO: GC
 */
class LocalTransfers
{
    private static final Logger logger = LoggerFactory.getLogger(LocalTransfers.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<TimeUUID, CoordinatedTransfer> coordinating = new HashMap<>();
    private final Map<ShortMutationId, CoordinatedTransfer> coordinatingActivated = new HashMap<>();
    private final Map<TimeUUID, PendingLocalTransfer> received = new HashMap<>();

    void save(CoordinatedTransfer transfer)
    {
        lock.writeLock().lock();
        try
        {
            CoordinatedTransfer existing = coordinating.put(transfer.transferId, transfer);
            Preconditions.checkState(existing == null);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void activating(CoordinatedTransfer transfer)
    {
        lock.writeLock().lock();
        try
        {
            Preconditions.checkState(transfer.activationId == null);
            transfer.activationId = transfer.getActivationId.get();

            coordinatingActivated.put(transfer.activationId, transfer);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void received(PendingLocalTransfer transfer)
    {
        logger.debug("received: {}", transfer);
        Preconditions.checkState(!transfer.sstables.isEmpty());

        lock.writeLock().lock();
        try
        {
            PendingLocalTransfer existing = received.put(transfer.planId, transfer);
            Preconditions.checkState(existing == null);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    PendingLocalTransfer getPendingTransfer(TimeUUID planId)
    {
        return checkNotNull(received.get(planId));
    }

    CoordinatedTransfer getActivatedTransfer(ShortMutationId activationId)
    {
        return checkNotNull(coordinatingActivated.get(activationId));
    }

    public void streamUnreconciledTransfers(InetAddressAndPort to)
    {
        List<Future<Void>> streams = new ArrayList<>();
        coordinating.forEach((transferId, transfer) -> {
            Optional<TimeUUID> planId = transfer.streams.get(to);
            if (planId == null || planId.isPresent())
                return;

            logger.debug("Found unreconciled stream to {}: {}", transfer, to);
            streams.add(transfer.stream(to));
        });
        // How to handle stream failing?
        FutureCombiner.successfulOf(streams).awaitUninterruptibly();
    }
}
