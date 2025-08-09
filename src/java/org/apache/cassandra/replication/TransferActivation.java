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

import com.google.common.base.Preconditions;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.utils.TimeUUID;

public class TransferActivation
{
    private final TimeUUID planId;
    private final MutationId transferId;

    TransferActivation(CoordinatedTransfer transfer)
    {
        this(transfer.planId, transfer.transferId);
    }

    TransferActivation(TimeUUID planId, MutationId transferId)
    {
        Preconditions.checkArgument(!transferId.isNone());
        Preconditions.checkNotNull(planId);
        this.planId = planId;
        this.transferId = transferId;
    }

    public void apply()
    {
        MutationTrackingService.instance.activatePendingTransfer(planId, transferId);
    }

    public static final Serializer serializer = new Serializer();

    public static class Serializer implements IVersionedSerializer<TransferActivation>
    {
        @Override
        public void serialize(TransferActivation activate, DataOutputPlus out, int version) throws IOException
        {
            TimeUUID.Serializer.instance.serialize(activate.planId, out, version);
            MutationId.serializer.serialize(activate.transferId, out, version);
        }

        @Override
        public TransferActivation deserialize(DataInputPlus in, int version) throws IOException
        {
            TimeUUID planId = TimeUUID.Serializer.instance.deserialize(in, version);
            MutationId transferId = MutationId.serializer.deserialize(in, version);
            return new TransferActivation(planId, transferId);
        }

        @Override
        public long serializedSize(TransferActivation activate, int version)
        {
            long size = 0;
            size += TimeUUID.Serializer.instance.serializedSize(activate.planId, version);
            size += MutationId.serializer.serializedSize(activate.transferId, version);
            return size;
        }
    }

    public static final IVerbHandler<TransferActivation> verbHandler = new IVerbHandler<>()
    {
        @Override
        public void doVerb(Message<TransferActivation> msg) throws IOException
        {
            msg.payload.apply();
        }
    };

    @Override
    public String toString()
    {
        return "Activate{" +
               "planId=" + planId +
               ", transferId=" + transferId +
               '}';
    }
}
