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
package com.expedia.rhapsody.kafka.factory;

import java.util.Collection;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactivestreams.Publisher;

import com.expedia.rhapsody.api.Acknowledgeable;
import com.expedia.rhapsody.core.transformer.AutoAcknowledgementConfig;
import com.expedia.rhapsody.kafka.acknowledgement.ReceiverAcknowledgementStrategy;

import reactor.core.publisher.Flux;

public class KafkaValueFluxFactory<V> extends KafkaFluxFactory<Object, V> {

    public KafkaValueFluxFactory(KafkaConfigFactory configFactory) {
        super(configFactory);
    }

    public Flux<Flux<V>> receiveAutoGroupValue(Collection<String> topics,
        AutoAcknowledgementConfig autoAcknowledgementConfig,
        Function<? super Flux<ConsumerRecord<Object, V>>, ? extends Publisher<ConsumerRecord<Object, V>>> pregroup) {
        return receiveAutoGroup(topics, autoAcknowledgementConfig, pregroup).map(this::extractNonNullValues);
    }

    public Flux<V> receiveAutoValue(Collection<String> topics, AutoAcknowledgementConfig autoAcknowledgementConfig) {
        return receiveAuto(topics, autoAcknowledgementConfig).transform(this::extractNonNullValues);
    }

    public Flux<Flux<Acknowledgeable<V>>> receiveGroupValue(Collection<String> topics,
        ReceiverAcknowledgementStrategy receiverAcknowledgementStrategy,
        Function<? super Flux<Acknowledgeable<ConsumerRecord<Object, V>>>, ? extends Publisher<Acknowledgeable<ConsumerRecord<Object, V>>>> pregroup) {
        return receiveGroup(topics, receiverAcknowledgementStrategy, pregroup).map(this::extractAcknowledgeableNonNullValues);
    }

    public Flux<Acknowledgeable<V>> receiveValue(Collection<String> topics, ReceiverAcknowledgementStrategy receiverAcknowledgementStrategy) {
        return receive(topics, receiverAcknowledgementStrategy).transform(this::extractAcknowledgeableNonNullValues);
    }

    private Flux<V> extractNonNullValues(Flux<? extends ConsumerRecord<Object, V>> consumerRecords) {
        return consumerRecords.filter(consumerRecord -> consumerRecord.value() != null).map(ConsumerRecord::value);
    }

    private Flux<Acknowledgeable<V>> extractAcknowledgeableNonNullValues(Flux<? extends Acknowledgeable<ConsumerRecord<Object, V>>> acknowledgeables) {
        return acknowledgeables
            .filter(Acknowledgeable.filtering(consumerRecord -> consumerRecord.value() != null, Acknowledgeable::acknowledge))
            .map(Acknowledgeable.mapping(ConsumerRecord::value));
    }
}
