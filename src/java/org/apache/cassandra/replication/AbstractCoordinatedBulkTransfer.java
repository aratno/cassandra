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
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import javax.annotation.CheckReturnValue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.RequestFailure;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static org.apache.cassandra.replication.AbstractCoordinatedBulkTransfer.SingleTransferResult.State.STREAM_COMPLETE;
import static org.apache.cassandra.replication.TransferActivation.Phase;

public abstract class AbstractCoordinatedBulkTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(AbstractCoordinatedBulkTransfer.class);

    String logPrefix()
    {
        return String.format("[%s #%s]", getClass().getSimpleName(), id);
    }

    private final ShortMutationId id;
    // TODO: Refactor to new class PendingTransfers
    final ConcurrentMap<InetAddressAndPort, SingleTransferResult> streamResults;

    public AbstractCoordinatedBulkTransfer(ShortMutationId id)
    {
        this.id = id;
        this.streamResults = new ConcurrentHashMap<>();
    }

    ShortMutationId id()
    {
        return id;
    }

    public boolean isCommitted()
    {
        for (SingleTransferResult result : streamResults.values())
        {
            if (result.state != SingleTransferResult.State.COMMITTED)
                return false;
        }
        return true;
    }

    final void activate(Collection<InetAddressAndPort> peers)
    {
        Preconditions.checkState(!peers.isEmpty());
        logger.debug("{} Activating {} on {}", logPrefix(), this, peers);
        LocalTransfers.instance().activating(this);

        // First phase ensures data is present on disk, then second phase does the actual import. This ensures that if
        // something goes wrong (like a topology change during import), we don't have divergence.
        class Prepare extends AsyncFuture<Void> implements RequestCallbackWithFailure<NoPayload>
        {
            final Set<InetAddressAndPort> responses = ConcurrentHashMap.newKeySet();

            public Prepare()
            {
                responses.addAll(peers);
            }

            @Override
            public void onResponse(Message<NoPayload> msg)
            {
                logger.debug("{} Got response from: {}", logPrefix(), msg.from());
                responses.remove(msg.from());
                if (responses.isEmpty())
                    trySuccess(null);
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                logger.debug("{} Got failure {} from {}", logPrefix(), failure, from);
                AbstractCoordinatedBulkTransfer.this.streamResults.computeIfPresent(from, (peer, result) -> result.prepareFailed());
                tryFailure(new RuntimeException("Tracked import failed during PREPARE on " + from + " due to " + failure.reason));
            }
        }

        Prepare prepare = new Prepare();
        for (InetAddressAndPort peer : peers)
        {
            TransferActivation activation = new TransferActivation(this, peer, Phase.PREPARE);
            Message<TransferActivation> msg = Message.out(Verb.TRACKED_TRANSFER_ACTIVATE_REQ, activation);
            logger.debug("{} Sending {} to peer {}", logPrefix(), activation, peer);
            MessagingService.instance().sendWithCallback(msg, peer, prepare);
            AbstractCoordinatedBulkTransfer.this.streamResults.computeIfPresent(peer, (peer0, result) -> result.preparing());
        }
        try
        {
            prepare.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            throw Throwables.unchecked(cause);
        }
        logger.debug("{} Activation prepare complete for {}", logPrefix(), peers);

        // Acknowledgement of activation is equivalent to a remote write acknowledgement. The imported SSTables
        // are now part of the live set, visible to reads.
        class Commit extends AsyncFuture<Void> implements RequestCallbackWithFailure<Void>
        {
            final Set<InetAddressAndPort> responses = ConcurrentHashMap.newKeySet();

            private Commit(Collection<InetAddressAndPort> peers)
            {
                responses.addAll(peers);
            }

            @Override
            public void onResponse(Message<Void> msg)
            {
                logger.debug("{} Activation successfully applied on {}", logPrefix(), msg.from());
                AbstractCoordinatedBulkTransfer.this.streamResults.computeIfPresent(msg.from(), (peer, result) -> result.committed());

                MutationTrackingService.instance.receivedActivationResponse(AbstractCoordinatedBulkTransfer.this, msg.from());
                responses.remove(msg.from());
                if (responses.isEmpty())
                {
                    // All activations complete, schedule cleanup to purge pending SSTables
                    LocalTransfers.instance().scheduleCleanup();
                    trySuccess(null);
                }
            }

            @Override
            public void onFailure(InetAddressAndPort from, RequestFailure failure)
            {
                logger.error("{} Failed activation on {} due to {}", logPrefix(), from, failure);
                MutationTrackingService.instance.retryFailedTransfer(AbstractCoordinatedBulkTransfer.this, from, failure.failure);
                // TODO(expected): should only fail if we don't meet requested CL
                tryFailure(new RuntimeException("Tracked import failed during COMMIT on " + from + " due to " + failure.reason));
            }
        }

        Commit commit = new Commit(peers);
        for (InetAddressAndPort peer : peers)
        {
            TransferActivation activation = new TransferActivation(this, peer, Phase.COMMIT);
            Message<TransferActivation> msg = Message.out(Verb.TRACKED_TRANSFER_ACTIVATE_REQ, activation);

            logger.debug("{} Sending {} to peer {}", logPrefix(), activation, peer);
            MessagingService.instance().sendWithCallback(msg, peer, commit);
            AbstractCoordinatedBulkTransfer.this.streamResults.computeIfPresent(peer, (peer0, result) -> result.committing());
        }

        try
        {
            commit.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            throw Throwables.unchecked(cause);
        }
        logger.debug("{} Activation commit complete for {}", logPrefix(), peers);
    }

    /**
     * Tracks the lifecycle of a transfer from the coordinator to a single replica, using a two-phase commit protocol:
     *
     * <ul>
     *   <li>{@link State#INIT}: Transfer created, not yet streaming.</li>
     *   <li>{@link State#STREAM_COMPLETE}: Streaming successful, SSTables received on replica in pending directory.</li>
     *   <li>{@link State#STREAM_NOOP}: No data streamed (e.g., SSTable contains no rows in target range).</li>
     *   <li>{@link State#STREAM_FAILED}: Streaming failed, may not have a streaming plan ID yet.</li>
     *   <li>{@link State#PREPARING}: Preparing for activation (first phase).</li>
     *   <li>{@link State#PREPARE_FAILED}: Prepare failed, aborting transfer.</li>
     *   <li>{@link State#COMMITTING}: Committing transferred SSTables from pending to live set (second phase).</li>
     *   <li>{@link State#COMMITTED}: Transfer commit acknowledged on coordinator. SSTables now live and visible to reads.</li>
     * </ul>
     *
     * <h3>Valid State Transitions:</h3>
     * <pre>
     *                                       ┌────────────────┐
     *                                       ↓                │
     *   INIT ──┬──→ STREAM_COMPLETE ──→ PREPARING ──┬──→ COMMITTING ──→ COMMITTED
     *          │                                    │
     *          ├──→ STREAM_NOOP                     └──→ PREPARE_FAILED
     *          │
     *          └──→ STREAM_FAILED
     * </pre>
     *
     * Failure states may be non-terminal if sufficient replicas reach successful states, depending on the transfer's
     * consistency level.
     */
    static class SingleTransferResult
    {
        enum State
        {
            INIT,
            STREAM_NOOP,
            STREAM_FAILED,
            STREAM_COMPLETE,
            PREPARING,
            PREPARE_FAILED,
            COMMITTING,
            COMMITTED;

            EnumSet<State> transitionFrom;

            static
            {
                INIT.transitionFrom = EnumSet.noneOf(State.class);
                STREAM_NOOP.transitionFrom = EnumSet.of(INIT);
                STREAM_FAILED.transitionFrom = EnumSet.of(INIT);
                STREAM_COMPLETE.transitionFrom = EnumSet.of(INIT);
                PREPARING.transitionFrom = EnumSet.of(STREAM_COMPLETE, COMMITTING);
                PREPARE_FAILED.transitionFrom = EnumSet.of(PREPARING);
                COMMITTING.transitionFrom = EnumSet.of(PREPARING);
                COMMITTED.transitionFrom = EnumSet.of(COMMITTING);
            }
        }

        final State state;
        private final TimeUUID planId;

        @VisibleForTesting
        SingleTransferResult(State state, TimeUUID planId)
        {
            this.state = state;
            this.planId = planId;
        }

        private boolean canTransition(State to)
        {
            return to.transitionFrom.contains(state);
        }

        public static SingleTransferResult Init()
        {
            return new SingleTransferResult(State.INIT, null);
        }

        @VisibleForTesting
        static SingleTransferResult StreamComplete(TimeUUID planId)
        {
            return new SingleTransferResult(STREAM_COMPLETE, planId);
        }

        @VisibleForTesting
        static SingleTransferResult Noop()
        {
            return new SingleTransferResult(State.STREAM_NOOP, null);
        }

        @CheckReturnValue
        private SingleTransferResult transition(State to, TimeUUID planId)
        {
            if (!canTransition(to))
            {
                logger.error("Ignoring invalid transition from {} to {}", state, to);
                return this;
            }
            // Don't overwrite if the stream succeeded but PREPARE failed, so we can clean up later
            return new SingleTransferResult(to, planId == null ? this.planId : planId);
        }

        @CheckReturnValue
        public SingleTransferResult streamFailed(TimeUUID planId)
        {
            return transition(State.STREAM_FAILED, planId);
        }

        @CheckReturnValue
        public SingleTransferResult preparing()
        {
            return transition(State.PREPARING, this.planId);
        }

        @CheckReturnValue
        public SingleTransferResult prepareFailed()
        {
            return transition(State.PREPARE_FAILED, this.planId);
        }

        @CheckReturnValue
        public SingleTransferResult committing()
        {
            return transition(State.COMMITTING, this.planId);
        }

        @CheckReturnValue
        public SingleTransferResult committed()
        {
            return transition(State.COMMITTED, this.planId);
        }

        public TimeUUID planId()
        {
            return planId;
        }

        @Override
        public String toString()
        {
            return "SingleTransferResult{" +
                   "state=" + state +
                   ", planId=" + planId +
                   '}';
        }
    }
}
