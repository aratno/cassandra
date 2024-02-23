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

package org.apache.cassandra.distributed.test;

import java.util.concurrent.Callable;

import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.reads.AbstractReadExecutor;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;

// This test is a demonstration of the concurrent access that causes an
// ArrayIndexOutOfBoundsException; see CASSANDRA-19427.
//
// Run this and see logs like:
//
// INFO  [node1_isolatedExecutor:2] node1 16:35:56,987 - ExampleTest.java:101 [INTERCEPT] makeRequests Read(distributed_test_keyspace.tbl columns=*/* rowFilter= limits= key=1 filter=slice(slices=ALL, reversed=false), nowInSec=1708637756) org.apache.cassandra.locator.AbstractReplicaCollection$$Lambda$39616/0x00000008005aa440@3afd83e4
// INFO  [node1_isolatedExecutor:2] node1 16:35:56,991 - ExampleTest.java:101 [INTERCEPT] makeRequests Read(distributed_test_keyspace.tbl columns=*/* rowFilter= limits= key=2 filter=slice(slices=ALL, reversed=false), nowInSec=1708637756) org.apache.cassandra.locator.AbstractReplicaCollection$$Lambda$39616/0x00000008005aa440@6a889b64
// INFO  [ReadStage-2] node1 16:35:56,991 - ExampleTest.java:116 [INTERCEPT] executeLocally org.apache.cassandra.db.ReadExecutionController@6945a549
// INFO  [ReadStage-2] node1 16:35:56,991 - ClientWarn.java:54 Warning: Test warning on local state org.apache.cassandra.service.ClientWarn$State@5ea6364c
// INFO  [main] <main> 16:35:57,008 - ExampleTest.java:73 Finished read
//
// See that the two ClientWarn logs reference the same ClientWarn.State
// (5ea6364c) and both try to submit the warning from two different threads
// (ReadStage-1 and ReadStage-2). This is grounds for a race condition,
// since ClientWarn.State uses an ArrayList to store warnings, and
// ArrayList isn't thread-safe.
public class ReproClientWarnConcurrentAccessTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(ReproClientWarnConcurrentAccessTest.class);

    @Test
    public void repro() throws Throwable
    {
        try (Cluster cluster = init(builder()
                                    .withInstanceInitializer(Initializer::install)
                                    .withNodes(3).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));

            // Write some rows
            logger.info("Starting writes");
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?, ?, ?)"), QUORUM, 1, 1, 1);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?, ?, ?)"), QUORUM, 2, 2, 2);

            // Read rows together
            logger.info("Starting read");
            cluster.coordinator(1).execute(withKeyspace("SELECT * FROM %s.tbl WHERE pk IN (?, ?)"), QUORUM, 1, 2);

            logger.info("Finished read");
        }
    }

    public static class Initializer
    {
        static void install(ClassLoader cl, int instance)
        {
            // Only run on the coordinator
            if (instance != 1)
                return;

            new ByteBuddy().rebase(AbstractReadExecutor.class)
                           .method(named("makeRequests"))
                           .intercept(MethodDelegation.to(Initializer.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);

            new ByteBuddy().rebase(ReadCommand.class)
                           .method(named("executeLocally"))
                           .intercept(MethodDelegation.to(Initializer.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static void makeRequests(ReadCommand readCommand, Iterable<Replica> replicas, @SuperCall Callable<Void> r)
        {
            logger.info("[INTERCEPT] makeRequests {} {}", readCommand, replicas);
            try
            {
                r.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }

        public static UnfilteredPartitionIterator executeLocally(ReadExecutionController executionController, @SuperCall Callable<UnfilteredPartitionIterator> r)
        {
            if (executionController.metadata().keyspace.equals(KEYSPACE))
            {
                logger.info("[INTERCEPT] executeLocally {}", executionController);
                ClientWarn.instance.warn("Test warning");
            }
            try
            {
                return r.call();
            }
            catch (Exception e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}