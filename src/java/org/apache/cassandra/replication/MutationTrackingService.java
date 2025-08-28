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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.concurrent.ScheduledExecutorPlus;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.lifecycle.SSTableIntervalTree;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.reads.tracked.TrackedLocalReads;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Interval;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.ExecutorFactory.SimulatorSemantics.NORMAL;

// TODO (expected): persistence (handle restarts)
// TODO (expected): handle topology changes
public class MutationTrackingService
{
    private static final Logger logger = LoggerFactory.getLogger(MutationTrackingService.class);
    public static final MutationTrackingService instance = new MutationTrackingService();

    private final TrackedLocalReads localReads = new TrackedLocalReads();
    private final ReplicatedOffsetsBroadcaster broadcaster = new ReplicatedOffsetsBroadcaster();
    private final ConcurrentHashMap<String, KeyspaceShards> shards = new ConcurrentHashMap<>();
    private final LocalTransfers transfers = new LocalTransfers();

    private volatile boolean started = false;

    private MutationTrackingService() {}

    // TODO (expected): implement a TCM ChangeListener
    public synchronized void start(ClusterMetadata metadata)
    {
        if (started)
            return;

        logger.info("Starting replication tracking service");

        for (KeyspaceMetadata keyspace : metadata.schema.getKeyspaces())
            if (keyspace.useMutationTracking())
                shards.put(keyspace.name, KeyspaceShards.make(keyspace, metadata, this::nextHostLogId));

        transfers.fetchUnreconciled();
        broadcaster.start();

        started = true;
    }

    public synchronized boolean isStarted()
    {
        return started;
    }

    public void shutdownBlocking() throws InterruptedException
    {
        localReads.shutdownBlocking();
        broadcaster.shutdown();
        broadcaster.awaitTermination(1, TimeUnit.MINUTES);
    }

    public TrackedLocalReads localReads()
    {
        return localReads;
    }

    public MutationId nextMutationId(String keyspace, Token token)
    {
        MutationId id = getOrCreate(keyspace).nextMutationId(token);
        logger.trace("Created new mutation id {}", id);
        return id;
    }

    public void receivedWriteResponse(String keyspace, Token token, MutationId mutationId, InetAddressAndPort onHost)
    {
        Preconditions.checkArgument(!mutationId.isNone());
        getOrCreate(keyspace).receivedWriteResponse(token, mutationId, onHost);
    }

    public void receivedActivationAck(CoordinatedTransfer transfer, InetAddressAndPort onHost)
    {
        getOrCreate(transfer.keyspace).receivedActivationAck(transfer, onHost);
    }

    public void updateReplicatedOffsets(String keyspace, Range<Token> range, List<? extends Offsets> offsets, InetAddressAndPort onHost)
    {
        getOrCreate(keyspace).updateReplicatedOffsets(range, offsets, onHost);
    }

    public void startWriting(Mutation mutation)
    {
        Preconditions.checkArgument(!mutation.id().isNone());
        getOrCreate(mutation.getKeyspaceName()).startWriting(mutation);
    }

    public void finishWriting(Mutation mutation)
    {
        Preconditions.checkArgument(!mutation.id().isNone());
        getOrCreate(mutation.getKeyspaceName()).finishWriting(mutation);
    }

    public void executeTransfers(String keyspace, Set<SSTableReader> sstables, ConsistencyLevel cl)
    {
        logger.info("Creating tracked bulk transfers for keyspace {} sstables {}", keyspace, sstables);

        KeyspaceShards keyspaceShards = shards.get(keyspace);
        checkNotNull(keyspaceShards);

        CoordinatedTransfers transfers = CoordinatedTransfers.create(keyspaceShards, sstables);
        logger.info("Split input SSTables into transfers {}", transfers);

        for (CoordinatedTransfer transfer : transfers)
            transfer.execute(instance.transfers, cl);
    }

    public void received(PendingLocalTransfer transfer)
    {
        logger.debug("Received pending transfer for tracked table {}", transfer);
        instance.transfers.received(transfer);
    }

    void activateLocal(TransferActivation activation)
    {
        logger.debug("activateLocal {}", activation);

        PendingLocalTransfer pending = instance.transfers.getPendingTransfer(activation.planId);
        pending.activate(activation);

        if (!activation.dryRun)
        {
            shards.get(pending.keyspace).lookUp(pending.range).receivedActivationAck(activation.activationId, FBUtilities.getBroadcastAddressAndPort());
        }
    }

    public CoordinatedTransfer getActivatedTransfer(ShortMutationId activationId)
    {
        return instance.transfers.getActivatedTransfer(activationId);
    }

    public MutationSummary createSummaryForKey(DecoratedKey key, TableId tableId, boolean includePending)
    {
        return getOrCreate(tableId).createSummaryForKey(key, tableId, includePending);
    }

    public MutationSummary createSummaryForRange(AbstractBounds<PartitionPosition> range, TableId tableId, boolean includePending)
    {
        return getOrCreate(tableId).createSummaryForRange(range, tableId, includePending);
    }

    public MutationSummary createSummaryForRange(Range<Token> range, TableId tableId, boolean includePending)
    {
        return createSummaryForRange(Range.makeRowRange(range), tableId, includePending);
    }

    void forEachKeyspace(Consumer<KeyspaceShards> consumer)
    {
        for (KeyspaceShards keyspaceShards : shards.values())
            consumer.accept(keyspaceShards);
    }

    private KeyspaceShards getOrCreate(TableId tableId)
    {
        //noinspection DataFlowIssue
        return getOrCreate(Schema.instance.getTableMetadata(tableId).keyspace);
    }

    private KeyspaceShards getOrCreate(String keyspace)
    {
        KeyspaceShards ks = shards.get(keyspace);
        if (ks != null)
            return ks;

        ClusterMetadata csm = ClusterMetadata.current();
        KeyspaceMetadata ksm = csm.schema.getKeyspaceMetadata(keyspace);
        return shards.computeIfAbsent(keyspace, ignore -> KeyspaceShards.make(ksm, csm, this::nextHostLogId));
    }

    // TODO (expected): durability
    int nextHostLogId()
    {
        return nextHostLogId.incrementAndGet();
    }
    private final AtomicInteger nextHostLogId = new AtomicInteger();

    public boolean isDurablyReconciled(String keyspace, ImmutableCoordinatorLogOffsets logOffsets)
    {
        // Could pass through SSTable bounds to exclude shards for non-overlapping ranges, but this will mostly be
        // called on flush for L0 SSTables with wide bounds.

        KeyspaceShards keyspaceShards = shards.get(keyspace);
        if (keyspaceShards == null)
        {
            logger.debug("Could not find shards for keyspace {}", keyspace);
            return false;
        }

        for (Long logId : logOffsets)
        {
            CoordinatorLogId coordinatorLogId = new CoordinatorLogId(logId);
            CoordinatorLog log = keyspaceShards.logs.get(coordinatorLogId);
            if (log == null)
            {
                logger.warn("Could not determine lifecycle for unknown logId {}, not marking as durably reconciled", coordinatorLogId);
                return false;
            }
            if (!log.isDurablyReconciled(logOffsets))
                return false;
        }

        return true;
    }

    private static class KeyspaceShards implements Shard.Subscriber
    {
        // TODO: private
        final String keyspace;
        private final Map<Range<Token>, Shard> shards;

        private transient final Map<Range<PartitionPosition>, Shard> ppShards;
        private transient final Map<CoordinatorLogId, CoordinatorLog> logs;

        static KeyspaceShards make(KeyspaceMetadata keyspace, ClusterMetadata cluster, IntSupplier logIdProvider)
        {
            Preconditions.checkArgument(keyspace.params.replicationType.isTracked());
            Map<Range<Token>, Shard> shards = new HashMap<>();
            cluster.placements.get(keyspace.params.replication).writes.forEach((tokenRange, forRange) -> {
                if (!forRange.endpoints().contains(FBUtilities.getBroadcastAddressAndPort()))
                    return;
                IntArrayList participants = new IntArrayList(forRange.size(), IntArrayList.DEFAULT_NULL_VALUE);
                for (InetAddressAndPort endpoint : forRange.endpoints())
                    participants.add(cluster.directory.peerId(endpoint).id());
                Shard shard = new Shard(keyspace.name, tokenRange, cluster.myNodeId().id(), new Participants(participants), forRange.lastModified(), logIdProvider);
                shards.put(tokenRange, shard);
            });
            return new KeyspaceShards(keyspace.name, shards);
        }

        KeyspaceShards(String keyspace, Map<Range<Token>, Shard> shards)
        {
            this.keyspace = keyspace;
            this.shards = shards;

            this.ppShards = new HashMap<>();
            this.logs = new HashMap<>();
            shards.forEach((range, shard) -> {
                ppShards.put(Range.makeRowRange(range), shard);
                shard.addSubscriber(this);
            });
        }

        MutationId nextMutationId(Token token)
        {
            return lookUp(token).nextId();
        }

        void receivedWriteResponse(Token token, MutationId mutationId, InetAddressAndPort onHost)
        {
            lookUp(token).receivedWriteResponse(mutationId, onHost);
        }

        void receivedActivationAck(CoordinatedTransfer transfer, InetAddressAndPort onHost)
        {
            logger.trace("receivedActivationAck {} {}", transfer, onHost);
            lookUp(transfer.range).receivedActivationAck(transfer.activationId, onHost);
        }

        void updateReplicatedOffsets(Range<Token> range, List<? extends Offsets> offsets, InetAddressAndPort onHost)
        {
            shards.get(range).updateReplicatedOffsets(offsets, onHost);
        }

        void startWriting(Mutation mutation)
        {
            lookUp(mutation).startWriting(mutation);
        }

        void finishWriting(Mutation mutation)
        {
            lookUp(mutation).finishWriting(mutation);
        }

        MutationSummary createSummaryForKey(DecoratedKey key, TableId tableId, boolean includePending)
        {
            MutationSummary.Builder builder = new MutationSummary.Builder(tableId);
            lookUp(key.getToken()).addSummaryForKey(key.getToken(), includePending, builder);

            /* REVIEW
            Like we do for data reads, summaries need to include the set of transfers they're aware of, in order to
            guarantee monotonic reads. Read coordinators need to know whether to read-reconcile and activate a pending
            transfer.

            I was thinking of doing that by fetching the View (volatile read) and loading all the relevant SSTables'
            transfer IDs would be one way to do that.

            The alternative is to integrate SSTable import with CoordinatorLog, and ensure that we atomically update
            the UnreconciledMutations and View, and avoid any tearing.
            */

            return builder.build();
        }

        MutationSummary createSummaryForRange(AbstractBounds<PartitionPosition> range, TableId tableId, boolean includePending)
        {
            MutationSummary.Builder builder = new MutationSummary.Builder(tableId);
            forEachIntersectingShard(range, shard -> shard.addSummaryForRange(range, includePending, builder));
            return builder.build();
        }

        private void forEachIntersectingShard(AbstractBounds<PartitionPosition> bounds, Consumer<Shard> consumer)
        {
            ppShards.forEach((range, shard) -> {
                // TODO (expected): partial workaround - is there a better way to do this?
                //  SELECT * statements create Bounds[min,min], (PartitionKeyRestrictions.java:L174) not Range(min,min],
                //  which Ranges generally won't intersect with (Range.java:L148), so contains is used here to make it work
                if (bounds.contains(range.right) || range.intersects(bounds))
                    consumer.accept(shard);
            });
        }

        void forEachShard(Consumer<Shard> consumer)
        {
            for (Shard shard : shards.values())
                consumer.accept(shard);
        }

        Shard lookUp(Mutation mutation)
        {
            return lookUp(mutation.key());
        }

        Shard lookUp(DecoratedKey key)
        {
            return lookUp(key.getToken());
        }

        Shard lookUp(Token token)
        {
            ClusterMetadata csm = ClusterMetadata.current();
            KeyspaceMetadata ksm = csm.schema.getKeyspaceMetadata(keyspace);
            Range<Token> range = ClusterMetadata.current().placements.get(ksm.params.replication).writes.forRange(token).range();
            return shards.get(range);
        }

        @Override
        public void onLogCreation(CoordinatorLog log)
        {
            logger.debug("Indexing created log {}", log);
            logs.put(log.logId, log);
        }

        @Override
        public void onSubscribe(CoordinatorLog currentLog)
        {
            logger.debug("Indexing current log {}", currentLog);
            logs.put(currentLog.logId, currentLog);
        }

        Shard lookUp(Range<Token> range)
        {
            ClusterMetadata csm = ClusterMetadata.current();
            KeyspaceMetadata ksm = csm.schema.getKeyspaceMetadata(keyspace);
            Range<Token> replicationRange = ClusterMetadata.current().placements.get(ksm.params.replication).writes.forRange(range).range();
            return shards.get(replicationRange);
        }
    }

    private static class CoordinatedTransfers implements Iterable<CoordinatedTransfer>
    {
        private final Collection<CoordinatedTransfer> transfers;

        private CoordinatedTransfers(Collection<CoordinatedTransfer> transfers)
        {
            this.transfers = transfers;
        }

        private static CoordinatedTransfers create(KeyspaceShards shards, Collection<SSTableReader> sstables)
        {
            // Clean up incoming SSTables to remove any existing CoordinatorLogOffsets, can't be trusted
            for (SSTableReader sstable : sstables)
            {
                try
                {
                    sstable.mutateCoordinatorLogOffsetsAndReload(ImmutableCoordinatorLogOffsets.NONE);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }

            // Expensive - add a metric?
            // TODO(expected): Fail if incoming transfer is outside owned shard ranges
            SSTableIntervalTree intervals = SSTableIntervalTree.buildSSTableIntervalTree(sstables);
            List<CoordinatedTransfer> transfers = new ArrayList<>();

            String keyspace = shards.keyspace;
            shards.forEachShard(shard -> {
                Range<Token> range = shard.tokenRange;
                Collection<SSTableReader> sstablesForRange = intervals.search(Interval.create(range.left.minKeyBound(), range.right.maxKeyBound()));

                CoordinatedTransfer transfer = new CoordinatedTransfer(keyspace, range, shard.participants, sstablesForRange, shard::nextId);
                transfers.add(transfer);

                /* REVIEW NOTES
                Right now for simplicity, streaming from coordinator to itself instead of copying files. This has some
                perks: (1) it allows us to import out-of-range SSTables using the same paths, and (2) it uses the
                existing lifecycle management to handle crash-safety, so don't need to deal with atomic multi-file copy.
                */
            });
            return new CoordinatedTransfers(transfers);
        }

        @Override
        public Iterator<CoordinatedTransfer> iterator()
        {
            return transfers.iterator();
        }

        public int size()
        {
            return transfers.size();
        }
    }

    // TODO (later): a more intelligent heuristic for offsets included in broadcasts
    private static class ReplicatedOffsetsBroadcaster implements Runnable, Shutdownable
    {
        private static final ScheduledExecutorPlus executor =
            executorFactory().scheduled("Replicated-Offsets-Broadcaster", NORMAL);

        // TODO (later): a more intelligent heuristic for scheduling broadcasts
        private static final long BROADCAST_INTERVAL_MILLIS = 200;

        void start()
        {
            executor.scheduleWithFixedDelay(this, BROADCAST_INTERVAL_MILLIS, BROADCAST_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
        }

        @Override
        public boolean isTerminated()
        {
            return executor.isTerminated();
        }

        @Override
        public void shutdown()
        {
            executor.shutdown();
        }

        @Override
        public Object shutdownNow()
        {
            return executor.shutdownNow();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
        {
            return executor.awaitTermination(timeout, units);
        }

        @Override
        public void run()
        {
            MutationTrackingService.instance.forEachKeyspace(this::run);
        }

        private void run(KeyspaceShards shards)
        {
            shards.forEachShard(this::run);
        }

        private void run(Shard shard)
        {
            ShardReplicatedOffsets replicatedOffsets = shard.collectReplicatedOffsets();
            if (replicatedOffsets.isEmpty())
                return;

            Message<ShardReplicatedOffsets> message = Message.out(Verb.BROADCAST_LOG_OFFSETS, replicatedOffsets);

            for (InetAddressAndPort target : shard.remoteReplicas())
                if (FailureDetector.instance.isAlive(target))
                    MessagingService.instance().send(message, target);
        }
    }

    @VisibleForTesting
    public void broadcastOffsetsForTesting()
    {
        broadcaster.run();
    }
}
