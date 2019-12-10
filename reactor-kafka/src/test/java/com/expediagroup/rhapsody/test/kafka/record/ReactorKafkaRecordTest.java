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
package com.expediagroup.rhapsody.test.kafka.record;

import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;
import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.core.adapter.Adapters;
import com.expediagroup.rhapsody.core.transformer.AutoAcknowledgementConfig;
import com.expediagroup.rhapsody.kafka.avro.test.TestSchemaRegistryFactory;
import com.expediagroup.rhapsody.kafka.extractor.ConsumerRecordExtraction;
import com.expediagroup.rhapsody.kafka.factory.KafkaConfigFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaFluxFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueSenderFactory;
import com.expediagroup.rhapsody.kafka.sending.FailureLoggingSenderSubscriber;
import com.expediagroup.rhapsody.kafka.test.TestKafkaFactory;
import com.expediagroup.rhapsody.test.TestHeaded;
import com.expediagroup.rhapsody.test.kafka.util.TestKafkaConfigFactory;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertEquals;

public class ReactorKafkaRecordTest {

    private static final Map<String, ?> KAFKA_CONFIG = new TestKafkaFactory().createKafka();

    private static final URL SCHEMA_REGISTRY_URL = new TestSchemaRegistryFactory().createConnect();

    private static final KafkaConfigFactory KAFKA_CONFIG_FACTORY =
        TestKafkaConfigFactory.createFactory(TestKafkaFactory.extractConnect(KAFKA_CONFIG), SCHEMA_REGISTRY_URL.toString());

    private static final String TOPIC = ReactorKafkaRecordTest.class.getSimpleName();

    private final Consumer<TestHeaded> consumer = Adapters.toSendingConsumer(createSender());

    private final Flux<ConsumerRecord<String, TestHeaded>> recordFlux = createPublisher();

    @Test
    public void consumedRecordsMatchSentValues() {
        Map<String, String> headers = Collections.singletonMap("DIO", "CAN_YOU_HEAR_ME");

        TestHeaded testHeaded = new TestHeaded(headers, "DATA");
        consumer.accept(testHeaded);

        StepVerifier.create(recordFlux)
            .consumeNextWith(record -> {
                assertEquals(headers, ConsumerRecordExtraction.extractHeaderMap(record));
                assertEquals(testHeaded, record.value());
            })
            .thenCancel()
            .verify();
    }

    private static Consumer<Publisher<TestHeaded>> createSender() {
        return values -> new KafkaValueSenderFactory<TestHeaded>(KAFKA_CONFIG_FACTORY)
            .sendValues(values, work -> TOPIC, TestHeaded::getData)
            .subscribe(new FailureLoggingSenderSubscriber<>());
    }

    private static Flux<ConsumerRecord<String, TestHeaded>> createPublisher() {
        return new KafkaFluxFactory<String, TestHeaded>(KAFKA_CONFIG_FACTORY)
            .receiveAuto(Collections.singletonList(TOPIC), new AutoAcknowledgementConfig());
    }
}