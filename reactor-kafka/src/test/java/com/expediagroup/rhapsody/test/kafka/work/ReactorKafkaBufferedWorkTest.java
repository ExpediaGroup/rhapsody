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
package com.expediagroup.rhapsody.test.kafka.work;

import java.net.URL;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;

import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.core.adapter.Adapters;
import com.expediagroup.rhapsody.core.transformer.AutoAcknowledgementConfig;
import com.expediagroup.rhapsody.kafka.avro.test.TestSchemaRegistryFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaConfigFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueFluxFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueSenderFactory;
import com.expediagroup.rhapsody.kafka.sending.FailureLoggingSenderSubscriber;
import com.expediagroup.rhapsody.kafka.test.TestKafkaFactory;
import com.expediagroup.rhapsody.test.TestWork;
import com.expediagroup.rhapsody.test.core.work.AbstractBufferedWorkTest;
import com.expediagroup.rhapsody.test.kafka.util.TestKafkaConfigFactory;

import reactor.core.publisher.Flux;

public class ReactorKafkaBufferedWorkTest extends AbstractBufferedWorkTest {

    private static final Map<String, ?> KAFKA_CONFIG = new TestKafkaFactory().createKafka();

    private static final URL SCHEMA_REGISTRY_URL = new TestSchemaRegistryFactory().createConnect();

    public ReactorKafkaBufferedWorkTest() {
        this(TestKafkaConfigFactory.createFactory(TestKafkaFactory.extractConnect(KAFKA_CONFIG), SCHEMA_REGISTRY_URL.toString()), TestWork.class.getSimpleName());
    }

    protected ReactorKafkaBufferedWorkTest(KafkaConfigFactory kafkaConfigFactory, String topic) {
        super(() -> Adapters.toSendingSubscriber(createSender(kafkaConfigFactory, topic)), () -> createPublisher(kafkaConfigFactory, topic));
    }

    private static Consumer<Publisher<TestWork>> createSender(KafkaConfigFactory kafkaConfigFactory, String topic) {
        return values -> new KafkaValueSenderFactory<TestWork>(kafkaConfigFactory)
            .sendValues(values, work -> topic, work -> work.workHeader().subject())
            .subscribe(new FailureLoggingSenderSubscriber<>());
    }

    private static Flux<TestWork> createPublisher(KafkaConfigFactory kafkaConfigFactory, String topic) {
        return new KafkaValueFluxFactory<TestWork>(kafkaConfigFactory)
            .receiveAutoValue(Collections.singletonList(topic), new AutoAcknowledgementConfig());
    }
}
