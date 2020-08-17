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
package com.expediagroup.rhapsody.samples.metrics;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.core.transformer.MetricsConfig;
import com.expediagroup.rhapsody.core.transformer.MetricsTransformer;
import com.expediagroup.rhapsody.kafka.acknowledgement.OrderManagingReceiverAcknowledgementStrategy;
import com.expediagroup.rhapsody.kafka.factory.KafkaConfigFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueFluxFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueSenderFactory;
import com.expediagroup.rhapsody.kafka.metrics.ReactiveStreamsMetricsReporter;
import com.expediagroup.rhapsody.kafka.test.TestKafkaFactory;

import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import reactor.core.publisher.Flux;

public class KafkaMetrics {

    private static final Map<String, ?> TEST_KAFKA_CONFIG = new TestKafkaFactory().createKafka();

    private static final String TOPIC_1 = "TOPIC_1";

    private static final String TOPIC_2 = "TOPIC_2";

    public static void main(String[] args) throws Exception {
        //Step 1) Create Kafka Producer Config for Producer that backs Sender's Subscriber
        //implementation. Note we are adding a Micrometer Metrics Reporter such that
        KafkaConfigFactory kafkaSubscriberConfig = new KafkaConfigFactory();
        kafkaSubscriberConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaSubscriberConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaMetrics.class.getSimpleName());
        kafkaSubscriberConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaSubscriberConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        kafkaSubscriberConfig.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, ReactiveStreamsMetricsReporter.class.getName());

        //Step 2) Create Kafka Consumer Config for Consumer that backs Receiver's Publisher
        //implementation
        KafkaConfigFactory kafkaPublisherConfig = new KafkaConfigFactory();
        kafkaPublisherConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaPublisherConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaMetrics.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaMetrics.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaPublisherConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaPublisherConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaPublisherConfig.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, ReactiveStreamsMetricsReporter.class.getName());

        //Step 3) Create a Meter registry which we'll add to the global Composite Registry (used by
        //default in resources configured above) and use in transformation later on
        MeterRegistry meterRegistry = new SimpleMeterRegistry();
        Metrics.addRegistry(meterRegistry);

        //Step 4) Create Metrics Config for the Metrics transformation we'll add later on
        MetricsConfig metricsConfig = new MetricsConfig(KafkaMetrics.class.getSimpleName(), Tags.of("test", "test"));

        //Step 5) Produce values to the topic we'll process
        new KafkaValueSenderFactory<String>(kafkaSubscriberConfig)
            .sendValues(Flux.just("test"), value -> TOPIC_1, Function.identity())
            .subscribe();

        //Step 6) Apply stream processing to the Kafka topic we produced records to
        new KafkaValueFluxFactory<String>(kafkaPublisherConfig)
            .receiveValue(Collections.singletonList(TOPIC_1), new OrderManagingReceiverAcknowledgementStrategy())
            .transform(new MetricsTransformer<>(metricsConfig, meterRegistry))
            .map(Acknowledgeable.mapping(String::toUpperCase))
            .transform(new KafkaValueSenderFactory<String>(kafkaSubscriberConfig).sendAcknowledgeableValues(TOPIC_2, Function.identity()))
            .doOnNext(Acknowledgeable::acknowledge)
            .map(Acknowledgeable::get)
            .publish()
            .autoConnect(0)
            .take(1)
            .collectList()
            .doOnNext(processedSenderResults -> System.out.println("processedSenderResults: " + processedSenderResults))
            .block();

        //Step 7) There are quite a few Metrics reported by Kafka, so we'll apply some filtering
        //for "interesting" Meters for the sake of brevity in this sample. You can modify or remove
        //this filtering to see the complete set of Metrics available
        List<Meter> interestingMeters = meterRegistry.getMeters().stream()
            .filter(KafkaMetrics::doesMeterHaveInterestingId)
            .filter(KafkaMetrics::doesMeterHaveInterestingMeasurement)
            .sorted(Comparator.comparing(meter -> meter.getId().getName()))
            .collect(Collectors.toList());

        //The following will print Meters now contained in the Registry we created above
        System.out.println("Number of (Interesting) Meters: " + interestingMeters.size());
        for (int i = 0; i < interestingMeters.size(); i++) {
            System.out.println(String.format("================ Meter #%d ================", i + 1));
            System.out.println("Meter Name: " + interestingMeters.get(i).getId().getName());
            System.out.println("Meter Type: " + interestingMeters.get(i).getId().getType());
            System.out.println("Meter Tags: " + interestingMeters.get(i).getId().getTags());
            interestingMeters.get(i).measure().forEach(measurement ->
                System.out.println("Meter Measurement: " + measurement.toString()));
        }

        System.exit(0);
    }

    private static boolean doesMeterHaveInterestingId(Meter meter) {
        return meter.getId().getName().contains(KafkaMetrics.class.getSimpleName()) ||
            meter.getId().getName().contains("lag");
    }

    private static boolean doesMeterHaveInterestingMeasurement(Meter meter) {
        for (Measurement measurement : meter.measure()) {
            if (!Double.isNaN(measurement.getValue()) && Double.isFinite(measurement.getValue())) {
                return true;
            }
        }
        return false;
    }
}
