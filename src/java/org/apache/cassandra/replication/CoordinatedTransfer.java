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

    // TODO(expected): Add epoch at time of creation
    public final Range<Token> range;
    public final Collection<InetAddressAndPort> participants;
    private final Collection<SSTableReader> sstables;

    public volatile TimeUUID planId = null;
    public volatile MutationId transferId = MutationId.none();

    CoordinatedTransfer(Range<Token> range, Participants participants, Collection<SSTableReader> sstables)
    {
        this.range = range;
        this.sstables = sstables;
        
        // TODO: Improve
        ClusterMetadata cm = ClusterMetadata.current();
        this.participants = new ArrayList<>(participants.size());
        for (int i = 0; i < participants.size(); i++)
        {
            InetAddressAndPort addr = cm.directory.getNodeAddresses(new NodeId(participants.get(i))).broadcastAddress;
            this.participants.add(addr);
        }
    }

    public void setTransferId(MutationId transferId)
    {
        Preconditions.checkState(this.transferId.isNone());
        logger.debug("Assigning Transfer ID {} for transfer {}", transferId, this);
        this.transferId = transferId;
    }

    /**
     * Returns whether any streaming actually happened. If not, there's nothing to activate.
     */
    public boolean stream()
    {
        StreamPlan plan = new StreamPlan(StreamOperation.IMPORT);

        // No need to flush, only using non-live SSTables already on disk
        plan.flushBeforeTransfer(false);

        for (SSTableReader sstable : sstables)
        {
            List<Range<Token>> ranges = Collections.singletonList(range);
            List<SSTableReader.PartitionPositionBounds> positions = sstable.getPositionsForRanges(ranges);
            long estimatedKeys = sstable.estimatedKeysForRanges(ranges);
            for (InetAddressAndPort addr : participants)
            {
                OutgoingStream stream = new CassandraOutgoingFile(StreamOperation.IMPORT, sstable.ref(), positions, ranges, estimatedKeys);
                plan.transferStreams(addr, Collections.singleton(stream));
            }
        }

        logger.info("Streaming transfer {}", this);
        StreamResultFuture execute = plan.execute();
        StreamState state;
        try
        {
            state = execute.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new RuntimeException(e);
        }

        // Still not exactly sure why this happens. My best guess is that thet SSTable doesn't contain any rows in the
        // provided range.
        if (state.sessions.isEmpty())
            return false;

        if (state.hasFailedSession() || state.hasAbortedSession())
            throw new RuntimeException("Stream failed due to failed or aborted sessions: " + state.sessions());

        this.planId = plan.planId();
        return true;
    }

    public Future<Void> activate()
    {
        // First phase is dryRun to ensure data is present on disk, then second phase does the actual import. This
        // ensures that if something goes wrong (like a topology change during import), we don't have divergence.
        return activate(new TransferActivation(this, true))
               .andThenAsync(prepared -> activate(new TransferActivation(this, false)));
    }

    private ActivationCallback activate(TransferActivation activation)
    {
        Message<TransferActivation> msg = Message.out(Verb.TRACKED_TRANSFER_ACTIVATE_REQ, activation);
        ActivationCallback cb = new ActivationCallback();

        for (InetAddressAndPort participant : participants)
        {
            logger.debug("Sending {} to peer {}", activation, participant);
            MessagingService.instance().sendWithCallback(msg, participant, cb);
        }
        return cb;
    }

    private class ActivationCallback extends AsyncFuture<Void> implements RequestCallbackWithFailure<NoPayload>
    {
        // TODO: Improve
        final ConcurrentMap<InetAddressAndPort, InetAddressAndPort> acks;

        public ActivationCallback()
        {
            ConcurrentMap<InetAddressAndPort, InetAddressAndPort> acks = new ConcurrentHashMap<>(CoordinatedTransfer.this.participants.size());
            for (InetAddressAndPort participant : CoordinatedTransfer.this.participants)
                acks.put(participant, participant);
            this.acks = acks;
        }

        @Override
        public void onResponse(Message<NoPayload> msg)
        {
            logger.debug("Activation success response: {}", msg.from());
            acks.remove(msg.from());
            if (acks.isEmpty())
                trySuccess(null);
        }

        @Override
        public void onFailure(InetAddressAndPort from, RequestFailureReason failureReason)
        {
            logger.debug("Activation failure response: {} {}", from, failureReason);
            tryFailure(null);
        }
    }

    @Override
    public String toString()
    {
        return "CoordinatedTransfer{" +
               "range=" + range +
               ", participants=" + participants +
               ", sstables=" + sstables +
               ", planId=" + planId +
               ", transferId=" + transferId +
               '}';
    }
}
