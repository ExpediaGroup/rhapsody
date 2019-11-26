/**
 * Copyright 2019 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.expedia.rhapsody.kafka.partitioning;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AllOrNothingYoungestInstanceWinsPartitionAssignorTest {

    private static final String MEMBER_ID_1 = "MemberId1";

    private static final String MEMBER_ID_2 = "MemberId2";

    private static final String TOPIC_1 = "topic-1";

    private static final String TOPIC_2 = "topic-2";

    private final AbstractPartitionAssignor partitionAssignor = new AllOrNothingPartitionAssignor();

    @Test
    public void emptySubscriptionsResultInNoAssignments() {
        Map<String, List<TopicPartition>> result = partitionAssignor.assign(
            Collections.singletonMap(TOPIC_1, 10),
            Collections.singletonMap(MEMBER_ID_1, new PartitionAssignor.Subscription(Collections.emptyList())));

        assertEquals(1, result.size());
        assertTrue(result.get(MEMBER_ID_1).isEmpty());
    }

    @Test
    public void zeroPartitionsResultInNoAssignments() {
        Map<String, List<TopicPartition>> result = partitionAssignor.assign(
            Collections.singletonMap(TOPIC_1, 0),
            Collections.singletonMap(MEMBER_ID_1, new PartitionAssignor.Subscription(Collections.singletonList(TOPIC_1))));

        assertEquals(1, result.size());
        assertTrue(result.get(MEMBER_ID_1).isEmpty());
    }

    @Test
    public void singleTopicIsAssignedToSingleMember() {
        Map<String, List<TopicPartition>> result = partitionAssignor.assign(
            Collections.singletonMap(TOPIC_1, 10),
            Collections.singletonMap(MEMBER_ID_1, new PartitionAssignor.Subscription(Collections.singletonList(TOPIC_1))));

        assertEquals(1, result.size());
        assertEquals(10, result.get(MEMBER_ID_1).size());
    }

    @Test
    public void mutuallyExclusiveSubscriptionsAreAssignedToMembers() {
        Map<String, Integer> partitionCountsByTopic = new HashMap<>();
        partitionCountsByTopic.put(TOPIC_1, 10);
        partitionCountsByTopic.put(TOPIC_2, 20);

        Map<String, PartitionAssignor.Subscription> subscriptionsByMemberId = new LinkedHashMap<>();
        subscriptionsByMemberId.put(MEMBER_ID_1, new PartitionAssignor.Subscription(Collections.singletonList(TOPIC_1)));
        subscriptionsByMemberId.put(MEMBER_ID_2, new PartitionAssignor.Subscription(Collections.singletonList(TOPIC_2)));

        Map<String, List<TopicPartition>> result = partitionAssignor.assign(partitionCountsByTopic, subscriptionsByMemberId);

        assertEquals(2, result.size());
        assertEquals(10, result.get(MEMBER_ID_1).size());
        assertEquals(20, result.get(MEMBER_ID_2).size());
    }

    @Test
    public void fullyOverlappingSubscriptionsWithSameMemberAgesAreAssignedToAlphabeticallyLastMember() {
        Instant now = Instant.now();
        Map<String, Integer> partitionCountsByTopic = new HashMap<>();
        partitionCountsByTopic.put(TOPIC_1, 10);

        Map<String, PartitionAssignor.Subscription> subscriptionsByMemberId = new LinkedHashMap<>();
        subscriptionsByMemberId.put(MEMBER_ID_1, new PartitionAssignor.Subscription(Collections.singletonList(TOPIC_1), AllOrNothingPartitionAssignor.serializeBorn(now)));
        subscriptionsByMemberId.put(MEMBER_ID_2, new PartitionAssignor.Subscription(Collections.singletonList(TOPIC_1), AllOrNothingPartitionAssignor.serializeBorn(now)));

        Map<String, List<TopicPartition>> result = partitionAssignor.assign(partitionCountsByTopic, subscriptionsByMemberId);

        assertEquals(2, result.size());
        assertEquals(10, result.get(MEMBER_ID_2).size());
        assertTrue(result.get(MEMBER_ID_1).isEmpty());
    }

    @Test
    public void fullyOverlappingSubscriptionsAreAssignedToYoungestMember() {
        Instant now = Instant.now();
        Map<String, Integer> partitionCountsByTopic = new HashMap<>();
        partitionCountsByTopic.put(TOPIC_1, 10);

        Map<String, PartitionAssignor.Subscription> subscriptionsByMemberId = new LinkedHashMap<>();
        subscriptionsByMemberId.put(MEMBER_ID_1, new PartitionAssignor.Subscription(
            Collections.singletonList(TOPIC_1), AllOrNothingPartitionAssignor.serializeBorn(now)));
        subscriptionsByMemberId.put(MEMBER_ID_2, new PartitionAssignor.Subscription(
            Collections.singletonList(TOPIC_1), AllOrNothingPartitionAssignor.serializeBorn(now.minusMillis(1L))));

        Map<String, List<TopicPartition>> result = partitionAssignor.assign(partitionCountsByTopic, subscriptionsByMemberId);

        assertEquals(2, result.size());
        assertEquals(10, result.get(MEMBER_ID_1).size());
        assertTrue(result.get(MEMBER_ID_2).isEmpty());
    }

    @Test
    public void partiallyOverlappingSubscriptionsAreAssignedToYoungestMember() {
        Instant now = Instant.now();
        Map<String, Integer> partitionCountsByTopic = new HashMap<>();
        partitionCountsByTopic.put(TOPIC_1, 10);
        partitionCountsByTopic.put(TOPIC_2, 20);

        Map<String, PartitionAssignor.Subscription> subscriptionsByMemberId = new LinkedHashMap<>();
        subscriptionsByMemberId.put(MEMBER_ID_1, new PartitionAssignor.Subscription(
            Collections.singletonList(TOPIC_1), AllOrNothingPartitionAssignor.serializeBorn(now)));
        subscriptionsByMemberId.put(MEMBER_ID_2, new PartitionAssignor.Subscription(
            Arrays.asList(TOPIC_1, TOPIC_2), AllOrNothingPartitionAssignor.serializeBorn(now.minusMillis(1L))));

        Map<String, List<TopicPartition>> result = partitionAssignor.assign(partitionCountsByTopic, subscriptionsByMemberId);

        assertEquals(2, result.size());
        assertEquals(10, result.get(MEMBER_ID_1).size());
        assertEquals(20, result.get(MEMBER_ID_2).size());
    }
}