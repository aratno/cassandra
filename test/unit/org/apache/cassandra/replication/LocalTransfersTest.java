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

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.TimeUUID;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.replication.AbstractCoordinatedBulkTransfer.SingleTransferResult.State.COMMITTED;
import static org.apache.cassandra.replication.AbstractCoordinatedBulkTransfer.SingleTransferResult.State.COMMITTING;
import static org.apache.cassandra.replication.AbstractCoordinatedBulkTransfer.SingleTransferResult.State.PREPARE_FAILED;
import static org.apache.cassandra.replication.AbstractCoordinatedBulkTransfer.SingleTransferResult.State.PREPARING;
import static org.apache.cassandra.replication.AbstractCoordinatedBulkTransfer.SingleTransferResult.State.STREAM_COMPLETE;
import static org.apache.cassandra.replication.AbstractCoordinatedBulkTransfer.SingleTransferResult.State.STREAM_FAILED;
import static org.apache.cassandra.replication.AbstractCoordinatedBulkTransfer.SingleTransferResult.State.STREAM_NOOP;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class LocalTransfersTest
{
    private LocalTransfers localTransfers;
    private ShortMutationId transferId;
    private TimeUUID planId;

    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
    }

    @Before
    public void setUp() throws Exception
    {
        localTransfers = new LocalTransfers();
        transferId = new ShortMutationId(1, 100);
        planId = nextTimeUUID();
    }

    private TrackedImportTransfer coordinatedTransfer(ShortMutationId transferId)
    {
        return coordinatedTransfer(transferId, new Range<>(tk(0), tk(1000)));
    }

    private TrackedImportTransfer coordinatedTransfer(ShortMutationId transferId, Range<Token> range)
    {
        MutationId mutationId = transferId != null
            ? new MutationId(transferId.logId(), transferId.offset(), (int) System.currentTimeMillis())
            : null;
        return new TrackedImportTransfer(range, mutationId);
    }

    private PendingLocalTransfer pendingTransfer(TimeUUID planId)
    {
        SSTableReader mockSSTable = mock(SSTableReader.class);
        Collection<SSTableReader> sstables = Collections.singletonList(mockSSTable);

        return new PendingLocalTransfer(planId, sstables);
    }

    private static Token tk(long token)
    {
        return new Murmur3Partitioner.LongToken(token);
    }

    @Test
    public void testSaveCoordinatedTransfer()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        localTransfers.save(transfer);
        localTransfers.activating(transfer);

        AbstractCoordinatedBulkTransfer loaded = localTransfers.getActivatedTransfer(transferId);
        Assertions.assertThat(loaded).isEqualTo(transfer);

        assertThatThrownBy(() -> localTransfers.save(transfer))
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testActivatingTransfer()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        localTransfers.save(transfer);
        localTransfers.activating(transfer);

        AbstractCoordinatedBulkTransfer retrieved = localTransfers.getActivatedTransfer(transferId);
        assertThat(retrieved).isEqualTo(transfer);
    }

    @Test
    public void testReceivedTransfer()
    {
        TimeUUID planId = nextTimeUUID();
        PendingLocalTransfer transfer = pendingTransfer(planId);
        localTransfers.received(transfer);
        PendingLocalTransfer retrieved = localTransfers.getPendingTransfer(planId);
        assertThat(retrieved).isEqualTo(transfer);
    }

    @Test
    public void testReceivedEmptyTransferThrows()
    {
        assertThatThrownBy(() -> new PendingLocalTransfer(planId, Collections.emptyList()))
            .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testGetPendingTransferNotFound()
    {
        PendingLocalTransfer retrieved = localTransfers.getPendingTransfer(planId);
        assertThat(retrieved).isNull();
    }

    @Test
    public void testGetActivatedTransferNotFound()
    {
        AbstractCoordinatedBulkTransfer retrieved = localTransfers.getActivatedTransfer(transferId);
        assertThat(retrieved).isNull();
    }

    @Test
    public void testPurgingTransferNotStarted()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        // All streams in INIT state - should NOT be purgeable (stream hasn't started yet)
        TrackedImportTransfer.SingleTransferResult result = TrackedImportTransfer.SingleTransferResult.Init();
        transfer.streamResults.put(mock(InetAddressAndPort.class), result);

        Assertions.assertThat(localTransfers.purger.test(transfer)).isFalse();
    }

    @Test
    public void testPurgingTransferAllStreamsComplete()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        // All streams in STREAM_COMPLETE state - should NOT be purgeable (no failures)
        TrackedImportTransfer.SingleTransferResult result1 = TrackedImportTransfer.SingleTransferResult.StreamComplete(nextTimeUUID());
        TrackedImportTransfer.SingleTransferResult result2 = TrackedImportTransfer.SingleTransferResult.StreamComplete(nextTimeUUID());

        transfer.streamResults.put(mock(InetAddressAndPort.class), result1);
        transfer.streamResults.put(mock(InetAddressAndPort.class), result2);

        Assertions.assertThat(localTransfers.purger.test(transfer)).isFalse();
    }

    @Test
    public void testPurgingTransferPrepareFailed()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        TrackedImportTransfer.SingleTransferResult result1 = new TrackedImportTransfer.SingleTransferResult(PREPARE_FAILED, planId);
        TrackedImportTransfer.SingleTransferResult result2 = new TrackedImportTransfer.SingleTransferResult(PREPARING, planId);

        transfer.streamResults.put(mock(InetAddressAndPort.class), result1);
        transfer.streamResults.put(mock(InetAddressAndPort.class), result2);

        Assertions.assertThat(localTransfers.purger.test(transfer)).isTrue();
    }

    @Test
    public void testPurgingTransferAllActivationCommitted()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        // All streams in ACTIVATE_COMMITTED state - should be purgeable (allComplete = true)
        TrackedImportTransfer.SingleTransferResult result1 = new TrackedImportTransfer.SingleTransferResult(COMMITTED, planId);
        TrackedImportTransfer.SingleTransferResult result2 = new TrackedImportTransfer.SingleTransferResult(COMMITTED, planId);

        transfer.streamResults.put(mock(InetAddressAndPort.class), result1);
        transfer.streamResults.put(mock(InetAddressAndPort.class), result2);

        Assertions.assertThat(localTransfers.purger.test(transfer)).isTrue();
    }

    @Test
    public void testPurgingTransferMixedCommittedAndNoop()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        // Mix of ACTIVATE_COMMITTED and STREAM_NOOP - should be purgeable (allComplete = true)
        TrackedImportTransfer.SingleTransferResult result1 = new TrackedImportTransfer.SingleTransferResult(COMMITTED, planId);
        TrackedImportTransfer.SingleTransferResult result2 = new TrackedImportTransfer.SingleTransferResult(STREAM_NOOP, null);

        transfer.streamResults.put(mock(InetAddressAndPort.class), result1);
        transfer.streamResults.put(mock(InetAddressAndPort.class), result2);

        Assertions.assertThat(localTransfers.purger.test(transfer)).isTrue();
    }

    @Test
    public void testPurgingTransferActivationPartialCommitted()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        // One stream in ACTIVATE_PREPARING - should NOT be purgeable
        TrackedImportTransfer.SingleTransferResult result1 = new TrackedImportTransfer.SingleTransferResult(PREPARING, planId);
        TrackedImportTransfer.SingleTransferResult result2 = new TrackedImportTransfer.SingleTransferResult(COMMITTING, planId);

        transfer.streamResults.put(mock(InetAddressAndPort.class), result1);
        transfer.streamResults.put(mock(InetAddressAndPort.class), result2);

        Assertions.assertThat(localTransfers.purger.test(transfer)).isFalse();
    }

    @Test
    public void testPurgingTransferAllStreamsFailed()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        // All streams in STREAM_FAILED state - should be purgeable (noneActivated = true)
        TrackedImportTransfer.SingleTransferResult result1 = new TrackedImportTransfer.SingleTransferResult(STREAM_FAILED, planId);
        TrackedImportTransfer.SingleTransferResult result2 = new TrackedImportTransfer.SingleTransferResult(STREAM_FAILED, planId);

        transfer.streamResults.put(mock(InetAddressAndPort.class), result1);
        transfer.streamResults.put(mock(InetAddressAndPort.class), result2);

        Assertions.assertThat(localTransfers.purger.test(transfer)).isTrue();
    }

    @Test
    public void testPurgingTransferMixedInitAndFailed()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        // Mix of INIT and STREAM_FAILED - should be purgeable (has failure, none activated)
        TrackedImportTransfer.SingleTransferResult result1 = TrackedImportTransfer.SingleTransferResult.Init();
        TrackedImportTransfer.SingleTransferResult result2 = TrackedImportTransfer.SingleTransferResult.Init().streamFailed(nextTimeUUID());

        transfer.streamResults.put(mock(InetAddressAndPort.class), result1);
        transfer.streamResults.put(mock(InetAddressAndPort.class), result2);

        Assertions.assertThat(localTransfers.purger.test(transfer)).isTrue();
    }

    @Test
    public void testPurgingTransferMixedCompleteAndFailed()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        // Mix of STREAM_COMPLETE and STREAM_FAILED - should be purgeable (has failure, none activated)
        TrackedImportTransfer.SingleTransferResult result1 = TrackedImportTransfer.SingleTransferResult.StreamComplete(nextTimeUUID());
        TrackedImportTransfer.SingleTransferResult result2 = TrackedImportTransfer.SingleTransferResult.Init().streamFailed(nextTimeUUID());

        transfer.streamResults.put(mock(InetAddressAndPort.class), result1);
        transfer.streamResults.put(mock(InetAddressAndPort.class), result2);

        Assertions.assertThat(localTransfers.purger.test(transfer)).isTrue();
    }

    @Test
    public void testPurgingTransferMixedStreamingCompleteAndPreparing()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        // Mix of STREAM_COMPLETE and ACTIVATE_PREPARING - should NOT be purgeable
        // (noneActivated = false because of ACTIVATE_PREPARING, allComplete = false)
        TrackedImportTransfer.SingleTransferResult result1 = new TrackedImportTransfer.SingleTransferResult(STREAM_COMPLETE, planId);
        TrackedImportTransfer.SingleTransferResult result2 = new TrackedImportTransfer.SingleTransferResult(PREPARING, planId);

        transfer.streamResults.put(mock(InetAddressAndPort.class), result1);
        transfer.streamResults.put(mock(InetAddressAndPort.class), result2);

        Assertions.assertThat(localTransfers.purger.test(transfer)).isFalse();
    }

    @Test
    public void testPurgingTransferMixedCommittingCommitted()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        TrackedImportTransfer.SingleTransferResult result1 = new TrackedImportTransfer.SingleTransferResult(COMMITTING, planId);
        TrackedImportTransfer.SingleTransferResult result2 = new TrackedImportTransfer.SingleTransferResult(COMMITTED, planId);

        transfer.streamResults.put(mock(InetAddressAndPort.class), result1);
        transfer.streamResults.put(mock(InetAddressAndPort.class), result2);

        Assertions.assertThat(localTransfers.purger.test(transfer)).isFalse();
    }

    @Test
    public void testPurgingTransferWithNullTransferId()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(null);

        // All streams complete but transferId is null - should NOT be purgeable
        TrackedImportTransfer.SingleTransferResult result1 = new TrackedImportTransfer.SingleTransferResult(STREAM_COMPLETE, null);
        TrackedImportTransfer.SingleTransferResult result2 = new TrackedImportTransfer.SingleTransferResult(STREAM_COMPLETE, null);

        transfer.streamResults.put(mock(InetAddressAndPort.class), result1);
        transfer.streamResults.put(mock(InetAddressAndPort.class), result2);

        // allComplete = true, but transferId is null, so should not purge
        Assertions.assertThat(localTransfers.purger.test(transfer)).isFalse();
    }

    @Test
    public void testPurgingTransferNoopOnly()
    {
        TrackedImportTransfer transfer = coordinatedTransfer(transferId);

        // All streams in STREAM_NOOP - should be purgeable (both noneActivated and allComplete are true)
        TrackedImportTransfer.SingleTransferResult result1 = TrackedImportTransfer.SingleTransferResult.Noop();
        TrackedImportTransfer.SingleTransferResult result2 = TrackedImportTransfer.SingleTransferResult.Noop();

        transfer.streamResults.put(mock(InetAddressAndPort.class), result1);
        transfer.streamResults.put(mock(InetAddressAndPort.class), result2);

        Assertions.assertThat(localTransfers.purger.test(transfer)).isTrue();
    }
}
