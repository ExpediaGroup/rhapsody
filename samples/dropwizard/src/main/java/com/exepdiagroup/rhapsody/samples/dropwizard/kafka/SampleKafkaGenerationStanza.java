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
package com.exepdiagroup.rhapsody.samples.dropwizard.kafka;

import java.time.Duration;
import java.util.function.Function;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongSerializer;

import com.expediagroup.rhapsody.core.stanza.Stanza;
import com.expediagroup.rhapsody.kafka.factory.KafkaConfigFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueSenderFactory;
import com.expediagroup.rhapsody.kafka.metrics.ReactiveStreamsMetricsReporter;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

public class SampleKafkaGenerationStanza extends Stanza<SampleKafkaGenerationStanzaConfig> {

    @Override
    protected Disposable startDisposable(SampleKafkaGenerationStanzaConfig config) {
        KafkaConfigFactory kafkaSubscriberConfig = new KafkaConfigFactory();
        kafkaSubscriberConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        kafkaSubscriberConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, SampleKafkaGenerationStanza.class.getSimpleName());
        kafkaSubscriberConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaSubscriberConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        kafkaSubscriberConfig.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, ReactiveStreamsMetricsReporter.class.getName());

        KafkaValueSenderFactory<Long> kafkaValueSenderFactory = new KafkaValueSenderFactory<>(kafkaSubscriberConfig);

        return Flux.interval(Duration.ofMillis(100))
            .transform(longs -> kafkaValueSenderFactory.sendValues(longs, value -> config.getTopic(), Function.identity()))
            .subscribe();
    }
}
