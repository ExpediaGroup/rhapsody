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

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

/**
 * An Assignor that assigns all partitions for any given Topic to one-and-only-one Member. Useful
 * for a desired condition of having a single Producer of data derived from the consumption of a
 * topic
 */
public abstract class AbstractAllOrNothingPartitionAssignor extends AbstractPartitionAssignor {

    protected static final Instant INSTANCE_BORN = Instant.now();

    protected final Instant born;

    public AbstractAllOrNothingPartitionAssignor(Instant born) {
        this.born = born;
    }

    @Override
    public Subscription subscription(Set<String> topics) {
        return new Subscription(new ArrayList<>(topics), serializeBorn(born));
    }

    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionCountsByTopic, Map<String, Subscription> subscriptionsByMemberId) {
        Map<String, String> memberIdsByTopic = subscriptionsByMemberId.entrySet().stream()
            .map(entry -> createMemberIdTopics(entry.getKey(), entry.getValue()))
            .flatMap(Collection::stream)
            .collect(Collectors.toMap(MemberIdTopic::getTopic, MemberIdTopic::getMemberId, memberIdChooser(subscriptionsByMemberId)));

        Map<String, List<TopicPartition>> assignmentsByMemberId = memberIdsByTopic.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue,
                entry -> createTopicPartitions(entry.getKey(), partitionCountsByTopic.getOrDefault(entry.getKey(), 0)),
                this::mergeTopicPartitions));

        subscriptionsByMemberId.keySet().forEach(memberId -> {
            if (!assignmentsByMemberId.containsKey(memberId)) {
                assignmentsByMemberId.put(memberId, Collections.emptyList());
            }
        });

        return assignmentsByMemberId;
    }

    protected BinaryOperator<String> memberIdChooser(Map<String, Subscription> subscriptionsByMemberId) {
        Map<String, Long> bornEpochMilliByMemberId = subscriptionsByMemberId.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> extractBornEpochMilli(entry.getValue()).orElse(0L)));
        return bornMemberIdChooser(bornEpochMilliByMemberId);
    }

    protected abstract BinaryOperator<String> bornMemberIdChooser(Map<String, Long> bornEpochMilliByMemberId);

    protected static ByteBuffer serializeBorn(Instant born) {
        return serializeBornEpochMilli(born.toEpochMilli());
    }

    protected static ByteBuffer serializeBornEpochMilli(long bornEpochMilli) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(Long.BYTES);
        byteBuffer.putLong(0, bornEpochMilli);
        return byteBuffer;
    }

    protected static Optional<Long> extractBornEpochMilli(Subscription subscription) {
        return subscription.userData() != null && subscription.userData().remaining() == Long.BYTES ?
            Optional.of(deserializeBornEpochMilli(subscription.userData())) : Optional.empty();
    }

    protected static Long deserializeBornEpochMilli(ByteBuffer byteBuffer) {
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        longBuffer.putLong(byteBuffer.getLong());
        longBuffer.flip();
        return longBuffer.getLong();
    }

    private List<MemberIdTopic> createMemberIdTopics(String memberId, Subscription subscription) {
        return subscription.topics().stream()
            .map(topic -> new MemberIdTopic(memberId, topic))
            .collect(Collectors.toList());
    }

    private List<TopicPartition> createTopicPartitions(String topic, int partitionCount) {
        return IntStream.range(0, partitionCount)
            .mapToObj(partition -> new TopicPartition(topic, partition))
            .collect(Collectors.toList());
    }

    private List<TopicPartition> mergeTopicPartitions(List<TopicPartition> topicPartitions1, List<TopicPartition> topicPartitions2) {
        List<TopicPartition> merged = new ArrayList<>(topicPartitions1);
        merged.addAll(topicPartitions2);
        return merged;
    }

    private static final class MemberIdTopic {

        private final String memberId;

        private final String topic;

        public MemberIdTopic(String memberId, String topic) {
            this.memberId = memberId;
            this.topic = topic;
        }

        public String getMemberId() {
            return memberId;
        }

        public String getTopic() {
            return topic;
        }
    }
}
