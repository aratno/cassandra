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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.streaming.CassandraOutgoingFile;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.streaming.OutgoingStream;
import org.apache.cassandra.streaming.StreamException;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamPlan;
import org.apache.cassandra.streaming.StreamResultFuture;
import org.apache.cassandra.streaming.StreamState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.replication.AbstractCoordinatedBulkTransfer.SingleTransferResult.State.COMMITTED;
import static org.apache.cassandra.replication.AbstractCoordinatedBulkTransfer.SingleTransferResult.State.COMMITTING;
import static org.apache.cassandra.replication.AbstractCoordinatedBulkTransfer.SingleTransferResult.State.STREAM_COMPLETE;
import static org.apache.cassandra.replication.TransferActivation.Phase;

/**
 * Orchestrates the lifecycle of a tracked bulk data transfer for a single replica set, where the current instance is
 * coordinating the transfer.
 * <p>
 * The transfer proceeds through these phases:
 * <ol>
 *   <li>
 *       <b>Streaming</b>
 *       The coordinator streams SSTables to all replicas in parallel. Replicas store received data in a "pending"
 *       location where it's persisted to disk but not yet visible to reads. Once sufficient replicas have received
 *       their streams to meet the requested {@link ConsistencyLevel}, the SSTables are activated using a two-phase
 *       commit protocol, making them part of the live set and visible to reads.
 *   </li>
 *   <li>
 *       <b>Activation {@link Phase#PREPARE}</b>
 *       The coordinator sends PREPARE messages to verify replicas have the data persisted on disk and are ready for
 *       activation.
 *   </li>
 *   <li>
 *       <b>Activation {@link Phase#COMMIT}</b>
 *       After successful PREPARE, the coordinator sends COMMIT messages to replicas. Replicas atomically move data from
 *       pending to live sets, making it visible to reads with the proper transfer ID in metadata. If commit succeeds
 *       on some replicas but not others, the transfer will be activated later on via existing the existing
 *       reconciliation processes (read reconciliation and background reconciliation).
 *   </li>
 * </ol>
 *
 * For simplicity, the coordinator streams to itself rather than using direct file copy. This ensures we can use the
 * same lifecycle management for crash-safety and atomic add.
 * <p>
 * If a tracked data read is executed on a replica that's missing an activation, the read reconciliation process will
 * apply the missing activation during reconciliation and a subsequent read will succeed. To minimize the gap between
 * activations across replicas, avoid expensive operations like file copies or index builds during
 * {@link TransferActivation#apply()}.
 */
public class TrackedImportTransfer extends AbstractCoordinatedBulkTransfer
{
    private static final Logger logger = LoggerFactory.getLogger(TrackedImportTransfer.class);

    private final String keyspace;
    private final Range<Token> range;
    final Collection<SSTableReader> sstables;
    private final ConsistencyLevel cl;
    // TODO: Refactor to new class PendingTransfers
    final ConcurrentMap<InetAddressAndPort, SingleTransferResult> streamResults;

    @VisibleForTesting
    TrackedImportTransfer(Range<Token> range, MutationId id)
    {
        super(id);
        this.keyspace = null;
        this.range = range;
        this.sstables = Collections.emptyList();
        this.cl = null;
        this.streamResults = new ConcurrentHashMap<>();
    }

    TrackedImportTransfer(String keyspace, Range<Token> range, Participants participants, Collection<SSTableReader> sstables, ConsistencyLevel cl, Supplier<MutationId> nextId)
    {
        super(nextId.get());
        this.keyspace = keyspace;
        this.range = range;
        this.sstables = sstables;
        this.cl = cl;

        ClusterMetadata cm = ClusterMetadata.current();
        this.streamResults = new ConcurrentHashMap<>(participants.size());
        for (int i = 0; i < participants.size(); i++)
        {
            InetAddressAndPort addr = cm.directory.getNodeAddresses(new NodeId(participants.get(i))).broadcastAddress;
            this.streamResults.put(addr, SingleTransferResult.Init());
        }
    }

    void execute()
    {
        logger.debug("{} Executing tracked bulk transfer {}", logPrefix(), this);
        LocalTransfers.instance().save(this);
        stream();
    }

    private void stream()
    {
        // TODO: Don't stream multiple copies over the WAN, send one copy and indicate forwarding
        List<Future<Void>> streaming = new ArrayList<>(streamResults.size());
        for (InetAddressAndPort to : streamResults.keySet())
        {
            Future<Void> stream = LocalTransfers.instance().executor.submit(() -> {
                stream(to);
                return null;
            });
            streaming.add(stream);
        }

        // Wait for all streams to complete, so we can clean up after failures. If we exit at the first failure, a
        // future stream can complete.
        LinkedList<Throwable> failures = null;
        for (Future<Void> stream : streaming)
        {
            try
            {
                stream.get();
            }
            catch (InterruptedException | ExecutionException e)
            {
                if (failures == null)
                    failures = new LinkedList<>();
                failures.add(e);
                logger.error("{} Failed transfer due to", logPrefix(), e);
            }
        }

        if (failures != null && !failures.isEmpty())
        {
            Throwable failure = failures.element();
            Throwable cause = failure instanceof ExecutionException ? failure.getCause() : failure;
            maybeCleanupFailedStreams(cause);

            String msg = String.format("Failed streaming on %s instance(s): %s", failures.size(), failures);
            throw new RuntimeException(msg, Throwables.unchecked(cause));
        }

        logger.info("{} All streaming completed successfully", logPrefix());
    }

    private boolean sufficient()
    {
        AbstractReplicationStrategy ars = Keyspace.open(keyspace).getReplicationStrategy();
        int blockFor = cl.blockFor(ars);
        int responses = 0;
        for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streamResults.entrySet())
        {
            if (entry.getValue().state == STREAM_COMPLETE)
                responses++;
        }
        return responses >= blockFor;
    }

    void stream(InetAddressAndPort to)
    {
        SingleTransferResult result;
        try
        {
            result = streamTask(to);
        }
        catch (StreamException | ExecutionException | InterruptedException | TimeoutException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            markStreamFailure(to, cause);
            throw Throwables.unchecked(cause);
        }

        try
        {
            streamComplete(to, result);
        }
        catch (ExecutionException | InterruptedException | TimeoutException e)
        {
            Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
            throw Throwables.unchecked(cause);
        }
    }

    private void markStreamFailure(InetAddressAndPort to, Throwable cause)
    {
        TimeUUID planId;
        if (cause instanceof StreamException)
            planId = ((StreamException) cause).finalState.planId;
        else
            planId = null;
        streamResults.computeIfPresent(to, (peer, result) -> result.streamFailed(planId));
    }

    /**
     * This shouldn't throw an exception, even if we fail to notify peers of the streaming failure.
     */
    private void maybeCleanupFailedStreams(Throwable cause)
    {
        try
        {
            boolean purgeable = LocalTransfers.instance().purger.test(this);
            if (!purgeable)
                return;

            notifyFailure();
            LocalTransfers.instance().scheduleCleanup();
        }
        catch (Throwable t)
        {
            if (cause != null)
                t.addSuppressed(cause);
            logger.error("{} Failed to notify peers of stream failure", logPrefix(), t);
        }
    }

    private void streamComplete(InetAddressAndPort to, SingleTransferResult result) throws ExecutionException, InterruptedException, TimeoutException
    {
        streamResults.put(to, result);
        logger.info("{} Completed streaming to {}, {}", logPrefix(), to, this);
        maybeActivate();
    }

    synchronized void maybeActivate()
    {
        // If any activations have already been sent out, send new activations to any received plans that have not yet
        // been activated
        boolean anyActivated = false;
        Set<InetAddressAndPort> awaitingActivation = new HashSet<>();
        for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streamResults.entrySet())
        {
            InetAddressAndPort peer = entry.getKey();
            SingleTransferResult result = entry.getValue();
            if (result.state == COMMITTING || result.state == COMMITTED)
            {
                anyActivated = true;
            }
            else if (result.state == STREAM_COMPLETE)
                awaitingActivation.add(peer);
        }
        if (anyActivated && !awaitingActivation.isEmpty())
        {
            logger.debug("{} Transfer already activated on some peers, sending activations to remaining: {}", logPrefix(), awaitingActivation);
            activate(awaitingActivation);
            return;
        }
        // If no activations have been sent out, check whether we have enough planIds back to meet the required CL
        else if (sufficient())
        {
            Set<InetAddressAndPort> peers = new HashSet<>();
            for (Map.Entry<InetAddressAndPort, SingleTransferResult> entry : streamResults.entrySet())
            {
                InetAddressAndPort peer = entry.getKey();
                SingleTransferResult result = entry.getValue();
                if (result.state == STREAM_COMPLETE)
                    peers.add(peer);
            }
            logger.debug("{} Transfer meets consistency level {}, sending activations to {}", logPrefix(), cl, peers);
            activate(peers);
            return;
        }

        logger.debug("{} Nothing to activate", logPrefix());
    }

    public boolean isCommitted()
    {
        for (SingleTransferResult result : streamResults.values())
        {
            if (result.state != COMMITTED)
                return false;
        }
        return true;
    }

    private SingleTransferResult streamTask(InetAddressAndPort to) throws StreamException, ExecutionException, InterruptedException, TimeoutException
    {
        StreamPlan plan = new StreamPlan(StreamOperation.TRACKED_TRANSFER);

        // No need to flush, only using non-live SSTables already on disk
        plan.flushBeforeTransfer(false);

        for (SSTableReader sstable : sstables)
        {
            List<Range<Token>> ranges = Collections.singletonList(range);
            List<SSTableReader.PartitionPositionBounds> positions = sstable.getPositionsForRanges(ranges);
            long estimatedKeys = sstable.estimatedKeysForRanges(ranges);
            OutgoingStream stream = new CassandraOutgoingFile(StreamOperation.TRACKED_TRANSFER, sstable.ref(), positions, ranges, estimatedKeys);
            plan.transferStreams(to, Collections.singleton(stream));
        }

        long timeout = DatabaseDescriptor.getStreamTransferTaskTimeout().toMilliseconds();

        logger.info("{} Starting streaming transfer {} to peer {}", logPrefix(), this, to);
        StreamResultFuture execute = plan.execute();
        StreamState state;
        try
        {
            state = execute.get(timeout, TimeUnit.MILLISECONDS);
            logger.debug("{} Completed streaming transfer {} to peer {}", logPrefix(), this, to);
        }
        catch (InterruptedException | ExecutionException | TimeoutException e)
        {
            logger.error("Stream session failed with error", e);
            throw e;
        }

        if (state.hasFailedSession() || state.hasAbortedSession())
            throw new StreamException(state, "Stream failed due to failed or aborted sessions");

        // If the SSTable doesn't contain any rows in the provided range, no streams delivered, nothing to activate
        if (state.sessions().isEmpty())
            return SingleTransferResult.Noop();

        return SingleTransferResult.StreamComplete(plan.planId());
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == null || getClass() != o.getClass()) return false;
        TrackedImportTransfer that = (TrackedImportTransfer) o;
        return Objects.equals(keyspace, that.keyspace) && Objects.equals(range, that.range) && cl == that.cl && Objects.equals(streamResults, that.streamResults);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(keyspace, range, cl, streamResults);
    }

    @Override
    public String toString()
    {
        return "TrackedImportTransfer{" +
               "keyspace='" + keyspace + '\'' +
               ", range=" + range +
               ", cl=" + cl +
               ", streamResults=" + streamResults +
               ", sstables=" + sstables +
               ", streamResults=" + streamResults +
               '}';
    }
}
