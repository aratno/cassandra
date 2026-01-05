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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AlignedToShardBoundariesTest
{
    private static final String TEST_KEYSPACE = "test_ks";

    @BeforeClass
    public static void setUp() throws IOException
    {
        SchemaLoader.prepareServer();
    }

    private static Token tk(String key)
    {
        return new ByteOrderedPartitioner.BytesToken(ByteBufferUtil.bytes(key));
    }

    private static Range<Token> range(String left, String right)
    {
        return new Range<>(tk(left), tk(right));
    }

    @Test
    public void testNoShardsReturnsRangesAsSingleGroup()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // No shards set for keyspace
        Collection<Range<Token>> inputRanges = Arrays.asList(
            range("a", "m"),
            range("n", "z")
        );

        List<List<Range<Token>>> result = service.alignedToShardBoundaries(TEST_KEYSPACE, inputRanges);

        // Should return all ranges as a single group
        assertEquals(1, result.size());
        assertEquals(2, result.get(0).size());
        assertTrue(result.get(0).contains(range("a", "m")));
        assertTrue(result.get(0).contains(range("n", "z")));
    }

    @Test
    public void testSingleRangeWithinSingleShard()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create a single shard covering a-z
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "z"));
        MutationTrackingService.KeyspaceShards shards =
            MutationTrackingService.TestAccess.createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        MutationTrackingService.TestAccess.setKeyspaceShards(service, TEST_KEYSPACE, shards);

        // Input range completely within the shard
        Collection<Range<Token>> inputRanges = Collections.singletonList(range("d", "m"));

        List<List<Range<Token>>> result = service.alignedToShardBoundaries(TEST_KEYSPACE, inputRanges);

        // Should return the range unchanged in a single group
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).size());
        assertEquals(range("d", "m"), result.get(0).get(0));
    }

    @Test
    public void testSingleRangeSpanningMultipleShards()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create two shards
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "m"));
        shardRanges.add(range("m", "z"));
        MutationTrackingService.KeyspaceShards shards =
            MutationTrackingService.TestAccess.createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        MutationTrackingService.TestAccess.setKeyspaceShards(service, TEST_KEYSPACE, shards);

        // Input range spans both shards
        Collection<Range<Token>> inputRanges = Collections.singletonList(range("d", "s"));

        List<List<Range<Token>>> result = service.alignedToShardBoundaries(TEST_KEYSPACE, inputRanges);

        // Should be split into two groups, one per shard
        assertEquals(2, result.size());

        // Collect all split ranges
        List<Range<Token>> allRanges = new ArrayList<>();
        result.forEach(allRanges::addAll);
        assertEquals(2, allRanges.size());

        // Should contain the two split pieces
        assertTrue(allRanges.contains(range("d", "m")));
        assertTrue(allRanges.contains(range("m", "s")));
    }

    @Test
    public void testMultipleRangesWithinSameShard()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create a single shard
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "z"));
        MutationTrackingService.KeyspaceShards shards =
            MutationTrackingService.TestAccess.createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        MutationTrackingService.TestAccess.setKeyspaceShards(service, TEST_KEYSPACE, shards);

        // Multiple ranges all within the same shard
        Collection<Range<Token>> inputRanges = Arrays.asList(
            range("b", "d"),
            range("e", "g"),
            range("h", "j")
        );

        List<List<Range<Token>>> result = service.alignedToShardBoundaries(TEST_KEYSPACE, inputRanges);

        // All ranges should be in a single group (same shard)
        assertEquals(1, result.size());
        assertEquals(3, result.get(0).size());
        assertTrue(result.get(0).contains(range("b", "d")));
        assertTrue(result.get(0).contains(range("e", "g")));
        assertTrue(result.get(0).contains(range("h", "j")));
    }

    @Test
    public void testEmptyRangesReturnsEmptyOrSingleEmptyGroup()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create a shard
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "z"));
        MutationTrackingService.KeyspaceShards shards =
            MutationTrackingService.TestAccess.createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        MutationTrackingService.TestAccess.setKeyspaceShards(service, TEST_KEYSPACE, shards);

        // Empty input
        Collection<Range<Token>> inputRanges = Collections.emptyList();

        List<List<Range<Token>>> result = service.alignedToShardBoundaries(TEST_KEYSPACE, inputRanges);

        // Should return empty result (no groups with ranges)
        assertTrue(result.isEmpty() || (result.size() == 1 && result.get(0).isEmpty()));
    }

    @Test
    public void testMultipleRangesAcrossMultipleShards()
    {
        MutationTrackingService service = MutationTrackingService.TestAccess.create();

        // Create three shards
        Set<Range<Token>> shardRanges = new HashSet<>();
        shardRanges.add(range("a", "h"));
        shardRanges.add(range("h", "p"));
        shardRanges.add(range("p", "z"));
        MutationTrackingService.KeyspaceShards shards =
            MutationTrackingService.TestAccess.createTestKeyspaceShards(TEST_KEYSPACE, shardRanges);
        MutationTrackingService.TestAccess.setKeyspaceShards(service, TEST_KEYSPACE, shards);

        // Multiple ranges, some spanning shards
        Collection<Range<Token>> inputRanges = Arrays.asList(
            range("b", "e"),  // Within shard 1
            range("f", "j"),  // Spans shard 1 and 2
            range("q", "s")   // Within shard 3
        );

        List<List<Range<Token>>> result = service.alignedToShardBoundaries(TEST_KEYSPACE, inputRanges);

        // Should have 3 groups (one per shard)
        assertEquals(3, result.size());

        // Collect all split ranges
        List<Range<Token>> allRanges = new ArrayList<>();
        result.forEach(allRanges::addAll);

        // Should have: b-e, f-h (shard 1), h-j (shard 2), q-s (shard 3) = 4 ranges
        assertEquals(4, allRanges.size());
    }
}
