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
package com.expediagroup.rhapsody.kafka.partitioning;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.clients.consumer.internals.PartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AllOrNothingYoungestInstanceWinsPartitionAssignorTest {

    private static final String MEMBER_ID_1 = "MemberId1";

    private static final String MEMBER_ID_2 = "MemberId2";

    private static final String TOPIC_1 = "topic-1";

    private static final String TOPIC_2 = "topic-2";

    private static final Collection<PartitionInfo> PARTITION_INFOS = Stream.of(
        IntStream.range(0, 10).mapToObj(partition -> new PartitionInfo(TOPIC_1, partition, null, null, null)),
        IntStream.range(0, 20).mapToObj(partition -> new PartitionInfo(TOPIC_2, partition, null, null, null)))
        .flatMap(Function.identity())
        .collect(Collectors.toList());

    private static final Cluster CLUSTER =
        new Cluster("clusterId", Collections.emptyList(), PARTITION_INFOS, Collections.emptySet(), Collections.emptySet());

    private final AbstractPartitionAssignor partitionAssignor = new AllOrNothingYoungestInstanceWinsPartitionAssignor();

    @Test
    public void emptySubscriptionsResultInNoAssignments() {
        Map<String, PartitionAssignor.Subscription> subscriptionsByMemberId =
            Collections.singletonMap(MEMBER_ID_1, new PartitionAssignor.Subscription(Collections.emptyList()));

        Map<String, PartitionAssignor.Assignment> result =
            partitionAssignor.assign(CLUSTER, subscriptionsByMemberId);

        assertEquals(1, result.size());
        assertTrue(result.get(MEMBER_ID_1).partitions().isEmpty());
    }

    @Test
    public void singleTopicIsAssignedToSingleMember() {
        PartitionAssignor.Subscription subscription = partitionAssignor.subscription(Collections.singleton(TOPIC_1));

        Map<String, PartitionAssignor.Subscription> subscriptionsByMemberId =
            Collections.singletonMap(MEMBER_ID_1, subscription);

        Map<String, PartitionAssignor.Assignment> result =
            partitionAssignor.assign(CLUSTER, subscriptionsByMemberId);

        assertEquals(1, result.size());
        assertEquals(10, result.get(MEMBER_ID_1).partitions().size());
    }

    @Test
    public void mutuallyExclusiveSubscriptionsAreAssignedToMembers() {
        PartitionAssignor.Subscription subscription1 = partitionAssignor.subscription(Collections.singleton(TOPIC_1));
        PartitionAssignor.Subscription subscription2 = partitionAssignor.subscription(Collections.singleton(TOPIC_2));

        Map<String, PartitionAssignor.Subscription> subscriptionsByMemberId = new LinkedHashMap<>();
        subscriptionsByMemberId.put(MEMBER_ID_1, subscription1);
        subscriptionsByMemberId.put(MEMBER_ID_2, subscription2);

        Map<String, PartitionAssignor.Assignment> result =
            partitionAssignor.assign(CLUSTER, subscriptionsByMemberId);

        assertEquals(2, result.size());
        assertEquals(10, result.get(MEMBER_ID_1).partitions().size());
        assertEquals(20, result.get(MEMBER_ID_2).partitions().size());
    }

    @Test
    public void fullyOverlappingSubscriptionsWithSameMemberAgesAreAssignedToAlphabeticallyLastMember() {
        Instant now = Instant.now();
        PartitionAssignor.Subscription subscription1 = new PartitionAssignor.Subscription(
            Collections.singletonList(TOPIC_1), AbstractAllOrNothingPartitionAssignor.serializeBorn(now));
        PartitionAssignor.Subscription subscription2 = new PartitionAssignor.Subscription(
            Collections.singletonList(TOPIC_1), AbstractAllOrNothingPartitionAssignor.serializeBorn(now));

        Map<String, PartitionAssignor.Subscription> subscriptionsByMemberId = new LinkedHashMap<>();
        subscriptionsByMemberId.put(MEMBER_ID_1, subscription1);
        subscriptionsByMemberId.put(MEMBER_ID_2, subscription2);

        Map<String, PartitionAssignor.Assignment> result =
            partitionAssignor.assign(CLUSTER, subscriptionsByMemberId);

        assertEquals(2, result.size());
        assertEquals(10, result.get(MEMBER_ID_2).partitions().size());
        assertTrue(result.get(MEMBER_ID_1).partitions().isEmpty());
    }

    @Test
    public void fullyOverlappingSubscriptionsAreAssignedToYoungestMember() {
        Instant now = Instant.now();
        PartitionAssignor.Subscription subscription1 = new PartitionAssignor.Subscription(
            Collections.singletonList(TOPIC_1), AbstractAllOrNothingPartitionAssignor.serializeBorn(now));
        PartitionAssignor.Subscription subscription2 = new PartitionAssignor.Subscription(
            Collections.singletonList(TOPIC_1), AbstractAllOrNothingPartitionAssignor.serializeBorn(now.minusMillis(1L)));

        Map<String, PartitionAssignor.Subscription> subscriptionsByMemberId = new LinkedHashMap<>();
        subscriptionsByMemberId.put(MEMBER_ID_1, subscription1);
        subscriptionsByMemberId.put(MEMBER_ID_2, subscription2);

        Map<String, PartitionAssignor.Assignment> result =
            partitionAssignor.assign(CLUSTER, subscriptionsByMemberId);

        assertEquals(2, result.size());
        assertEquals(10, result.get(MEMBER_ID_1).partitions().size());
        assertTrue(result.get(MEMBER_ID_2).partitions().isEmpty());
    }

    @Test
    public void partiallyOverlappingSubscriptionsAreAssignedToYoungestMember() {
        Instant now = Instant.now();
        PartitionAssignor.Subscription subscription1 = new PartitionAssignor.Subscription(
            Collections.singletonList(TOPIC_1), AbstractAllOrNothingPartitionAssignor.serializeBorn(now));
        PartitionAssignor.Subscription subscription2 = new PartitionAssignor.Subscription(
            Arrays.asList(TOPIC_1, TOPIC_2), AbstractAllOrNothingPartitionAssignor.serializeBorn(now.minusMillis(1L)));

        Map<String, PartitionAssignor.Subscription> subscriptionsByMemberId = new LinkedHashMap<>();
        subscriptionsByMemberId.put(MEMBER_ID_1, subscription1);
        subscriptionsByMemberId.put(MEMBER_ID_2, subscription2);

        Map<String, PartitionAssignor.Assignment> result =
            partitionAssignor.assign(CLUSTER, subscriptionsByMemberId);

        assertEquals(2, result.size());
        assertEquals(10, result.get(MEMBER_ID_1).partitions().size());
        assertEquals(20, result.get(MEMBER_ID_2).partitions().size());
    }
}