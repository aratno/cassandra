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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.sstable.format.SSTableReader;
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

    // Map peer to streaming planId, null if no successful stream completed
    final Map<InetAddressAndPort, TimeUUID> streams;

    private final Collection<SSTableReader> sstables;

    // Assigned
    public volatile MutationId activationId = MutationId.none();

    CoordinatedTransfer(String keyspace, Range<Token> range, Participants participants, Collection<SSTableReader> sstables)
    {
        this.keyspace = keyspace;
        this.range = range;
        this.sstables = sstables;
        
        ClusterMetadata cm = ClusterMetadata.current();
        this.streams = new HashMap<>(participants.size());
        for (int i = 0; i < participants.size(); i++)
        {
            InetAddressAndPort addr = cm.directory.getNodeAddresses(new NodeId(participants.get(i))).broadcastAddress;
            this.streams.put(addr, null);
        }
    }

    public void setActivationId(MutationId activationId)
    {
        Preconditions.checkState(this.activationId.isNone());
        logger.debug("{} Assigning activationId {} for transfer {}", logPrefix(), activationId, this);
        this.activationId = activationId;
    }

    void stream(LocalTransfers transfers)
    {
        transfers.coordinating(this);

        // TODO: parallelize on streaming threads
        for (InetAddressAndPort peer : streams.keySet())
            stream(transfers, peer);

        /* TODO
        If some streams fail, that's OK. It just means we won't move on to activation.
        */
    }

    private void stream(LocalTransfers transfers, InetAddressAndPort peer)
    {
        StreamPlan plan = new StreamPlan(StreamOperation.IMPORT);

        // No need to flush, only using non-live SSTables already on disk
        plan.flushBeforeTransfer(false);

        for (SSTableReader sstable : sstables)
        {
            List<Range<Token>> ranges = Collections.singletonList(range);
            List<SSTableReader.PartitionPositionBounds> positions = sstable.getPositionsForRanges(ranges);
            long estimatedKeys = sstable.estimatedKeysForRanges(ranges);
            OutgoingStream stream = new CassandraOutgoingFile(StreamOperation.IMPORT, sstable.ref(), positions, ranges, estimatedKeys);
            plan.transferStreams(peer, Collections.singleton(stream));
        }

        logger.info("{} Starting streaming transfer {} to peer {}", logPrefix(), this, peer);
        StreamResultFuture execute = plan.execute();
        StreamState state;
        try
        {
            state = execute.get();
            logger.debug("{} Completed streaming transfer {} to peer {}", logPrefix(), this, peer);
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }

        if (state.hasFailedSession() || state.hasAbortedSession())
            throw new RuntimeException("Stream failed due to failed or aborted sessions: " + state.sessions());

        transfers.streamed(this, peer, plan.planId(), state);
    }

    Future<?> activate(LocalTransfers transfers, MutationId activationId)
    {
        transfers.activating(this, activationId);

        // First phase is dryRun to ensure data is present on disk, then second phase does the actual import. This
        // ensures that if something goes wrong (like a topology change during import), we don't have divergence.
        AllRespond allRespond = new AllRespond(streams.keySet());
        for (Map.Entry<InetAddressAndPort, TimeUUID> entry : streams.entrySet())
        {
            InetAddressAndPort peer = entry.getKey();
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
        for (Map.Entry<InetAddressAndPort, TimeUUID> entry : streams.entrySet())
        {
            InetAddressAndPort peer = entry.getKey();
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
        return ImmediateFuture.success(null);
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
