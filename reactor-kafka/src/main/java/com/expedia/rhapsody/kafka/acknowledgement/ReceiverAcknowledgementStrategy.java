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
package com.expedia.rhapsody.kafka.acknowledgement;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactivestreams.Publisher;

import com.expedia.rhapsody.api.Acknowledgeable;
import com.expedia.rhapsody.util.ConfigLoading;

import reactor.kafka.receiver.ReceiverRecord;

public interface ReceiverAcknowledgementStrategy {

    String MAX_IN_FLIGHT_PER_TOPIC_PARTITION_CONFIG = "max.in.flight.per.topic.partition";

    // Subscribers can control the number of "in-flight" (unacknowledged) Records emitted, with
    // Acknowledgement managed on a per-Topic-Partition basis. This can be helpful for controlling
    // memory usage and "Quality of Service"
    static Optional<Long> loadMaxInFlightPerTopicPartition(Map<String, ?> properties) {
        return ConfigLoading.load(properties, MAX_IN_FLIGHT_PER_TOPIC_PARTITION_CONFIG, Long::valueOf);
    }

    <K, V> Function<? super Publisher<ReceiverRecord<K, V>>, ? extends Publisher<Acknowledgeable<ConsumerRecord<K, V>>>>
    createRecordTransformer(Map<String, ?> properties);
}
