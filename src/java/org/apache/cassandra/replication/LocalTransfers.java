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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.repair.RepairJob;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.SyncStat;
import org.apache.cassandra.repair.SyncTask;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

/**
 * Singleton registry maintaining state for bulk data transfers on the local node.
 * <p>
 * This includes {@link TrackedImportTransfer} instances that the current node is coordinating, and
 * {@link PendingLocalTransfer} instances that are coordinated by other nodes. Pending transfers are inactive until
 * activated by the coordinator.
 * <p>
 * TODO: Make changes to pending set durable with SystemKeyspace.savePendingLocalTransfer(transfer)?
 * TODO: Add vtable for visibility into local and coordinated transfers
 * TODO: Rename this to TrackedTransferService?
 */
public class LocalTransfers
{
    private static final Logger logger = LoggerFactory.getLogger(LocalTransfers.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<ShortMutationId, AbstractCoordinatedBulkTransfer> coordinating = new ConcurrentHashMap<>();
    private final Map<TimeUUID, PendingLocalTransfer> local = new ConcurrentHashMap<>();

    final ExecutorPlus executor = executorFactory().pooled("LocalTrackedTransfers", Integer.MAX_VALUE);

    private static final LocalTransfers instance = new LocalTransfers();
    public static LocalTransfers instance()
    {
        return instance;
    }

    void save(TrackedImportTransfer transfer)
    {
        lock.writeLock().lock();
        try
        {
            AbstractCoordinatedBulkTransfer existing = coordinating.put(transfer.id(), transfer);
            Preconditions.checkState(existing == null);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void activating(AbstractCoordinatedBulkTransfer transfer)
    {
        Preconditions.checkNotNull(transfer.id());
        lock.writeLock().lock();
        try
        {
            coordinating.put(transfer.id(), transfer);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void received(PendingLocalTransfer transfer)
    {
        lock.writeLock().lock();
        try
        {
            logger.debug("received: {}", transfer);
            Preconditions.checkState(!transfer.sstables.isEmpty());

            PendingLocalTransfer existing = local.put(transfer.planId, transfer);
            Preconditions.checkState(existing == null);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    /**
     * We should track the repair as a CoordinatedLocalTransfer so when the sync is done we can either TransferActivation
     * or TransferFailed.
     *
     * Track before any of the sync tasks execute because we need to send {@link TransferFailed} to all replicas if a
     * failure happens.
     */
    public void onRepairSyncExecution(RepairJob job, RepairJobDesc desc, Collection<SyncTask> tasks)
    {
        // One RepairJob may have multiple TrackedRepairSyncTransfers, if it spans across shards.
        // Group tasks by their transfer ID and create one TrackedRepairSyncTransfer per unique ID.
        Map<ShortMutationId, List<SyncTask>> tasksByTransferId = new HashMap<>();
        for (SyncTask task : tasks)
        {
            MutationId transferId = task.getTransferId();
            if (transferId != null)
                tasksByTransferId.computeIfAbsent(transferId, k -> new ArrayList<>()).add(task);
        }

        // Create and register a TrackedRepairSyncTransfer for each unique transfer ID
        for (Map.Entry<ShortMutationId, List<SyncTask>> entry : tasksByTransferId.entrySet())
        {
            ShortMutationId transferId = entry.getKey();
            List<SyncTask> tasksForTransfer = entry.getValue();
            TrackedRepairSyncTransfer transfer = new TrackedRepairSyncTransfer(transferId, desc, tasksForTransfer);
            coordinating.put(transferId, transfer);
        }
    }

    /**
     * Begin activation for the sync'd transfer(s)
     */
    public void onRepairSyncCompletion(RepairJob job, Future<List<SyncStat>> syncCompletion, Executor executor)
    {
        syncCompletion.addCallback(new FutureCallback<List<SyncStat>>()
        {
            @Override
            public void onSuccess(List<SyncStat> syncs)
            {
                // Activation will acquire the write lock anyway, so don't self-deadlock
                lock.writeLock().lock();
                try
                {
                    logger.info("maybeActivate onSuccess {} {} {}", syncs, coordinating, local);

                    // Collect all unique transfer IDs from the job's sync tasks
                    Map<MutationId, List<SyncStat>> syncsByTransferId = new HashMap<>();

                    // Build a map of sync task ranges to their transfer IDs for lookup
                    Map<Collection<Range<Token>>, MutationId> rangeToTransferId = new HashMap<>();
                    for (SyncTask task : job.getSyncTasks())
                    {
                        MutationId transferId = task.getTransferId();
                        if (transferId != null)
                            rangeToTransferId.put(task.rangesToSync, transferId);
                    }

                    // Group sync stats by transfer ID based on their ranges
                    for (SyncStat sync : syncs)
                    {
                        MutationId transferId = rangeToTransferId.get(sync.differences);
                        if (transferId != null)
                            syncsByTransferId.computeIfAbsent(transferId, k -> new ArrayList<>()).add(sync);
                    }

                    // Activate each transfer with its corresponding sync stats
                    for (Map.Entry<MutationId, List<SyncStat>> entry : syncsByTransferId.entrySet())
                    {
                        MutationId transferId = entry.getKey();
                        List<SyncStat> syncsForTransfer = entry.getValue();

                        AbstractCoordinatedBulkTransfer transfer0 = coordinating.get(transferId);
                        Preconditions.checkState(transfer0 instanceof TrackedRepairSyncTransfer,
                                                 "Expected TrackedRepairSyncTransfer for %s but got %s",
                                                 transferId, transfer0);
                        TrackedRepairSyncTransfer transfer = (TrackedRepairSyncTransfer) transfer0;
                        transfer.activate(syncsForTransfer);
                    }
                }
                finally
                {
                    lock.writeLock().unlock();
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                logger.info("maybeActivate onFailure", t);
            }
        }, executor);
    }

    Purger purger = new Purger();

    static class Purger
    {
        /**
         * It's safe to purge a transfer if it failed either before it was activated anywhere, or after all activation
         * has completed everywhere. If a transfer is partially activated (on some replicas but not others), it's going
         * to be included in future reconciliations and needs to be preserved until reconciliation is complete.
         */
        boolean test(AbstractCoordinatedBulkTransfer transfer)
        {
            logger.debug("Checking whether we can purge {}", transfer);
            boolean failedBeforeActivation = false;
            boolean noneActivated = true;
            boolean allComplete = true;
            for (TrackedImportTransfer.SingleTransferResult result : transfer.streamResults.values())
            {
                switch (result.state)
                {
                    case STREAM_FAILED:
                    case PREPARE_FAILED:
                        failedBeforeActivation = true;
                        break;
                    case COMMITTED:
                        noneActivated = false;
                        break;
                    case INIT:
                    case STREAM_COMPLETE:
                    case PREPARING:
                    case COMMITTING:
                        allComplete = false;
                }
            }

            return (failedBeforeActivation && noneActivated) || allComplete;
        }

        boolean test(PendingLocalTransfer transfer)
        {
            return transfer.activated;
        }
    }

    private void cleanup()
    {
        lock.writeLock().lock();
        try
        {
            for (PendingLocalTransfer transfer : local.values())
                if (purger.test(transfer))
                    purge(transfer);

            for (AbstractCoordinatedBulkTransfer transfer : coordinating.values())
                if (purger.test(transfer))
                    purge(transfer);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private void purge(TransferFailed failed)
    {
        lock.writeLock().lock();
        try
        {
            PendingLocalTransfer pending = local.get(failed.planId);
            if (pending == null)
            {
                logger.warn("Cannot purge unknown local pending transfer {}", failed);
                return;
            }
            purge(pending);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private void purge(PendingLocalTransfer transfer)
    {
        logger.info("Cleaning up pending transfer {}", transfer);

        lock.writeLock().lock();
        try
        {
            // Delete the entire pending transfer directory /pending/<planId>/
            if (!transfer.sstables.isEmpty())
            {
                SSTableReader sstable = transfer.sstables.iterator().next();
                File pendingDir = sstable.descriptor.directory;

                if (pendingDir.exists())
                {
                    Preconditions.checkState(pendingDir.absolutePath().contains(transfer.planId.toString()));
                    logger.debug("Deleting pending transfer directory: {}", pendingDir);
                    pendingDir.deleteRecursive();
                }
            }
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    private void purge(AbstractCoordinatedBulkTransfer transfer)
    {
        logger.info("Cleaning up completed coordinated transfer: {}", transfer);

        lock.writeLock().lock();
        try
        {
            coordinating.remove(transfer.id());

            if (transfer.id() != null)
                coordinating.remove(transfer.id());

            TrackedImportTransfer.SingleTransferResult localPending = transfer.streamResults.get(FBUtilities.getBroadcastAddressAndPort());
            PendingLocalTransfer localTransfer;
            TimeUUID planId;
            if (localPending != null && (planId = localPending.planId()) != null && (localTransfer = local.get(planId)) != null)
                purge(localTransfer);
        }
        finally
        {
            lock.writeLock().unlock();
        }
    }

    void scheduleCleanup()
    {
        executor.submit(() -> {
            try
            {
                cleanup();
            }
            catch (Throwable t)
            {
                logger.error("Cleanup failed", t);
            }
        });
    }

    @Nullable PendingLocalTransfer getPendingTransfer(TimeUUID planId)
    {
        lock.readLock().lock();
        try
        {
            return local.get(planId);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    @Nullable
    AbstractCoordinatedBulkTransfer getActivatedTransfer(ShortMutationId transferId)
    {
        lock.readLock().lock();
        try
        {
            return coordinating.get(transferId);
        }
        finally
        {
            lock.readLock().unlock();
        }
    }

    public static IVerbHandler<TransferFailed> verbHandler = message -> {
        LocalTransfers.instance().purge(message.payload);
        MessagingService.instance().respond(NoPayload.noPayload, message);
    };
}
