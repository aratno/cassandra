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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.FutureTask;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

/**
 * A transfer for a single replica set.
 *
 * REVIEW: Right now for simplicity, streaming from coordinator to itself instead of copying files. This has some
 * perks: (1) it allows us to import out-of-range SSTables using the same paths, and (2) it uses the
 * existing lifecycle management to handle crash-safety, so don't need to deal with atomic multi-file copy.
 */
public class CoordinatedTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatedTransfer.class);

    private String logPrefix()
    {
        return String.format("[CoordinatedTransfer #%s]", transferId);
    }

    final TimeUUID transferId = TimeUUID.Generator.nextTimeUUID();

    // TODO(expected): Add epoch at time of creation
    final String keyspace;
    public final Range<Token> range;

    // Map peer to streaming planId if successful stream completed
    // TODO: move away from optional
    final ConcurrentMap<InetAddressAndPort, Optional<TimeUUID>> streams;

    private final Collection<SSTableReader> sstables;

    final Supplier<MutationId> getActivationId;
    volatile MutationId activationId = null;

    CoordinatedTransfer(String keyspace, Range<Token> range, Participants participants, Collection<SSTableReader> sstables, Supplier<MutationId> getActivationId)
    {
        this.keyspace = keyspace;
        this.range = range;
        this.sstables = sstables;
        this.getActivationId = getActivationId;

        ClusterMetadata cm = ClusterMetadata.current();
        this.streams = new ConcurrentHashMap<>(participants.size());
        for (int i = 0; i < participants.size(); i++)
        {
            InetAddressAndPort addr = cm.directory.getNodeAddresses(new NodeId(participants.get(i))).broadcastAddress;
            this.streams.put(addr, Optional.empty());
        }
    }

    void execute(LocalTransfers transfers, ConsistencyLevel cl)
    {
        logger.debug("Executing tracked bulk transfer {}", this);

        transfers.save(this);

        boolean complete = stream(cl);
        if (!complete)
            return;

        /* TODO
        If topology has changed after streaming, need to ensure new topology doesn't break consistency of completed
        streams.
        */
        transfers.activating(this);
        activate();
    }

    private boolean stream(ConsistencyLevel cl)
    {
        // TODO: Don't stream multiple copies over the WAN, send one copy and indicate forwarding
        List<Future<Void>> streaming = new ArrayList<>(streams.size());
        for (InetAddressAndPort to : streams.keySet())
            streaming.add(stream(to));

        try
        {
            FutureCombiner.successfulOf(streaming).get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }

        boolean sufficient = sufficient(cl);
        logger.debug("Sufficient responses to move on to activation? {}", sufficient);
        return sufficient;
    }

    private boolean sufficient(ConsistencyLevel cl)
    {
        AbstractReplicationStrategy ars = Keyspace.open(keyspace).getReplicationStrategy();
        int blockFor = cl.blockFor(ars);
        int responses = 0;
        for (Map.Entry<InetAddressAndPort, Optional<TimeUUID>> entry : streams.entrySet())
        {
            if (entry.getValue().isPresent())
                responses++;
        }
        return responses >= blockFor;
    }

    private Future<Void> stream(InetAddressAndPort to)
    {
        return streamTask(to).andThenAsync(planId -> {
            if (planId == null)
            {
                logger.debug("Empty stream to peer {}, skipping activation", to);
                streams.remove(to);
            }
            else
            {
                Optional<?> existing = streams.put(to, Optional.of(planId));
                Preconditions.checkState(existing != null && existing.isEmpty());
            }
            return ImmediateFuture.success(null);
        });
    }

    private Future<TimeUUID> streamTask(InetAddressAndPort to)
    {
        FutureTask<TimeUUID> task = new FutureTask<>(() -> {
            StreamPlan plan = new StreamPlan(StreamOperation.IMPORT);

            // No need to flush, only using non-live SSTables already on disk
            plan.flushBeforeTransfer(false);

            for (SSTableReader sstable : sstables)
            {
                List<Range<Token>> ranges = Collections.singletonList(range);
                List<SSTableReader.PartitionPositionBounds> positions = sstable.getPositionsForRanges(ranges);
                long estimatedKeys = sstable.estimatedKeysForRanges(ranges);
                OutgoingStream stream = new CassandraOutgoingFile(StreamOperation.IMPORT, sstable.ref(), positions, ranges, estimatedKeys);
                plan.transferStreams(to, Collections.singleton(stream));
            }

            logger.info("{} Starting streaming transfer {} to peer {}", logPrefix(), this, to);
            StreamResultFuture execute = plan.execute();
            StreamState state;
            try
            {
                state = execute.get();
                logger.debug("{} Completed streaming transfer {} to peer {}", logPrefix(), this, to);
            }
            catch (InterruptedException | ExecutionException e)
            {
                throw new RuntimeException(e);
            }

            if (state.hasFailedSession() || state.hasAbortedSession())
                throw new RuntimeException("Stream failed due to failed or aborted sessions: " + state.sessions());

            // If the SSTable doesn't contain any rows in the provided range, no streams delivered, nothing to activate
            if (state.sessions().isEmpty())
                return null;

            // TODO: execute streams in parallel
            return plan.planId();
        });
        Stage.ANTI_ENTROPY.submit(task);
        return task;
    }

    void activate()
    {
        Collection<InetAddressAndPort> acks = new ArrayList<>();
        streams.forEach((peer, planId) -> {
            if (planId.isPresent()) acks.add(peer);
        });

        // First phase is dryRun to ensure data is present on disk, then second phase does the actual import. This
        // ensures that if something goes wrong (like a topology change during import), we don't have divergence.
        AllRespond allRespond = new AllRespond(acks);
        for (InetAddressAndPort peer : acks)
        {
            TransferActivation activation = new TransferActivation(this, peer, true);
            Message<TransferActivation> msg = Message.out(Verb.TRACKED_TRANSFER_ACTIVATE_REQ, activation);
            for (InetAddressAndPort participant : streams.keySet())
            {
                logger.debug("{} Sending {} to peer {}", logPrefix(), activation, participant);
                MessagingService.instance().sendWithCallback(msg, participant, allRespond);
            }
        }
        allRespond.awaitUninterruptibly();

        // Acknowledgement of activation is equivalent to a remote write acknowledgement. The imported SSTables are now
        // part of the live set, visible to reads.
        for (InetAddressAndPort peer : acks)
        {
            TransferActivation activation = new TransferActivation(this, peer, false);
            Message<TransferActivation> msg = Message.out(Verb.TRACKED_TRANSFER_ACTIVATE_REQ, activation);

            RequestCallback<Void> callback = new RequestCallback<Void>()
            {
                @Override
                public void onResponse(Message<Void> msg)
                {
                    MutationTrackingService.instance.receivedActivationAck(CoordinatedTransfer.this, msg.from());
                }
            };

            for (InetAddressAndPort participant : streams.keySet())
            {
                logger.debug("{} Sending {} to peer {}", logPrefix(), activation, participant);
                MessagingService.instance().sendWithCallback(msg, participant, callback);
            }
        }

        /*
        When should this method return? We don't want to wait until all replicas have acknowledged the
        import, because import should tolerate nodes down.
        */
        ImmediateFuture.success(null);
    }

    private class AllRespond extends AsyncFuture<Void> implements RequestCallbackWithFailure<NoPayload>
    {
        final ConcurrentMap<InetAddressAndPort, InetAddressAndPort> acks;

        public AllRespond(Collection<InetAddressAndPort> acks)
        {
            ConcurrentHashMap<InetAddressAndPort, InetAddressAndPort> map = new ConcurrentHashMap<>(acks.size());
            for (InetAddressAndPort ack : acks)
                map.put(ack, ack);

            this.acks = map;
        }

        @Override
        public void onResponse(Message<NoPayload> msg)
        {
            logger.debug("{} Got response from: {}", logPrefix(), msg.from());
            acks.remove(msg.from());
            if (acks.isEmpty())
                trySuccess(null);
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
            logger.debug("{} Got failure {} from {}", logPrefix(), failureReason, from);
            tryFailure(null);
        }
    }

    @Override
    public String toString()
    {
        return "CoordinatedTransfer{" +
               "transferId=" + transferId +
               ", range=" + range +
               ", participants=" + streams +
               ", sstables=" + sstables +
               ", activationId=" + activationId +
               '}';
    }
}
