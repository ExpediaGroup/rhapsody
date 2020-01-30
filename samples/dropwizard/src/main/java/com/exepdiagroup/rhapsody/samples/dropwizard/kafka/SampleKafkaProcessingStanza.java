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

import java.util.Collections;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.core.stanza.Stanza;
import com.expediagroup.rhapsody.kafka.factory.KafkaConfigFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueFluxFactory;
import com.expediagroup.rhapsody.kafka.metrics.ReactiveStreamsMetricsReporter;

import reactor.core.Disposable;

public class SampleKafkaProcessingStanza extends Stanza<SampleKafkaProcessingStanzaConfig> {

    @Override
    protected Disposable startDisposable(SampleKafkaProcessingStanzaConfig config) {
        KafkaConfigFactory kafkaPublisherConfig = new KafkaConfigFactory();
        kafkaPublisherConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
        kafkaPublisherConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, SampleKafkaProcessingStanza.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.GROUP_ID_CONFIG, SampleKafkaProcessingStanza.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaPublisherConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaPublisherConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        kafkaPublisherConfig.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, ReactiveStreamsMetricsReporter.class.getName());

        return new KafkaValueFluxFactory<Long>(kafkaPublisherConfig)
            .receiveValue(Collections.singletonList(config.getTopic()))
            .filter(Acknowledgeable.filtering(this::isPrime, Acknowledgeable::acknowledge))
            .map(Acknowledgeable.mapping(primeNumber -> "Found a prime number: " + primeNumber))
            .subscribe(Acknowledgeable.consuming(config.getConsumer(), Acknowledgeable::acknowledge));
    }

    private boolean isPrime(Number number) {
        if (number.longValue() <= 1) {
            return false;
        }
        for (long i = 2; i <= Math.sqrt(number.doubleValue()); i++) {
            if (number.longValue() % i == 0) {
                return false;
            }
        }
        return true;
    }
}
