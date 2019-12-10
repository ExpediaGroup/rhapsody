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
package com.expediagroup.rhapsody.kafka.factory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.api.Headed;
import com.expediagroup.rhapsody.kafka.record.RecordHeaderConversion;

import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderResult;

public class KafkaValueSenderFactory<V> extends KafkaSenderFactory<Object, V> {

    public KafkaValueSenderFactory(KafkaConfigFactory configFactory) {
        super(configFactory);
    }

    public Flux<Acknowledgeable<SenderResult<V>>>
    sendAcknowledgeableValues(Publisher<Acknowledgeable<V>> acknowledgeableValues, Function<V, String> valueToTopic, Function<V, ?> valueToKey) {
        return sendAcknowledgeable(Flux.from(acknowledgeableValues)
            .map(acknowledgeable -> createAcknowledgeableProducerRecord(acknowledgeable, valueToTopic, valueToKey)));
    }

    public Flux<SenderResult<V>> sendValues(Publisher<V> values, Function<V, String> valueToTopic, Function<V, ?> valueToKey) {
        return send(Flux.from(values)
            .map(value -> new ProducerRecord<>(valueToTopic.apply(value), null, valueToKey.apply(value), value, extractHeaders(value))));
    }

    protected Acknowledgeable<ProducerRecord<Object, V>>
    createAcknowledgeableProducerRecord(Acknowledgeable<V> acknowledgeable, Function<V, String> valueToTopic, Function<V, ?> valueToKey) {
        return acknowledgeable.map(value -> new ProducerRecord<>(valueToTopic.apply(value), null, valueToKey.apply(value), value, createHeaders(acknowledgeable)));
    }

    protected List<Header> extractHeaders(V value) {
        return Headed.tryCast(value)
            .map(KafkaValueSenderFactory::createHeaders)
            .orElseGet(Collections::emptyList);
    }

    protected static List<Header> createHeaders(Headed headed) {
        return createHeaders(headed.header().toMap());
    }

    protected static List<Header> createHeaders(Map<String, String> headerMap) {
        return headerMap.entrySet().stream()
            .map(entry -> RecordHeaderConversion.toHeader(entry.getKey(), entry.getValue()))
            .collect(Collectors.toList());
    }
}
