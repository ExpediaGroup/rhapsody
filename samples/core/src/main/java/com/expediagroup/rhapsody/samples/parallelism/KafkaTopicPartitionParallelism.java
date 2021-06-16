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
package com.expediagroup.rhapsody.samples.parallelism;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.kafka.acknowledgement.OrderManagingReceiverAcknowledgementStrategy;
import com.expediagroup.rhapsody.kafka.factory.KafkaConfigFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaFluxFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueFluxFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueSenderFactory;
import com.expediagroup.rhapsody.kafka.test.TestKafkaFactory;
import com.expediagroup.rhapsody.util.Defaults;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * This sample demonstrates how to process Kafka records on a thread-per-topic-partition basis
 */
public class KafkaTopicPartitionParallelism {

    private static final Map<String, ?> TEST_KAFKA_CONFIG = new TestKafkaFactory().createKafka();

    private static final String TOPIC = "TOPIC";

    private static final int NUM_SAMPLES = 10000;

    private static final long MAX_SLEEP_MILLIS = 10;

    private static final Scheduler SCHEDULER = Schedulers.newBoundedElastic(
        Defaults.THREAD_CAP, Integer.MAX_VALUE, KafkaTopicPartitionParallelism.class.getSimpleName());

    public static void main(String[] args) throws Exception {
        //Step 1) Create Kafka Producer Config for Producer that backs Sender's Subscriber
        //implementation
        KafkaConfigFactory kafkaSubscriberConfig = new KafkaConfigFactory();
        kafkaSubscriberConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaSubscriberConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaTopicPartitionParallelism.class.getSimpleName());
        kafkaSubscriberConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaSubscriberConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        //Step 2) Create Kafka Consumer Config for Consumer that backs Receiver's Publisher
        //implementation. Note that we block our main Thread on partition assignment such that
        //subsequently produced Records are processed
        KafkaConfigFactory kafkaPublisherConfig = new KafkaConfigFactory();
        kafkaPublisherConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaPublisherConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaTopicPartitionParallelism.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaTopicPartitionParallelism.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaPublisherConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaPublisherConfig.put(KafkaFluxFactory.BLOCK_REQUEST_ON_PARTITION_ASSIGNMENT_CONFIG, true);

        //Step 3) Apply stream processing to the Kafka topic we'll produce records to. The
        //"processing" in this case introduces a superficial blocking sleep which might mimic an
        //IO-bound process.
        CountDownLatch latch = new CountDownLatch(NUM_SAMPLES);
        new KafkaValueFluxFactory<String>(kafkaPublisherConfig)
            .receiveGroupValue(Collections.singletonList(TOPIC), new OrderManagingReceiverAcknowledgementStrategy(), Function.identity())
            .subscribe(groupFlux -> groupFlux
                .publishOn(SCHEDULER)
                .map(Acknowledgeable.mapping(String::toUpperCase))
                .doOnNext(next -> {
                    try {
                        Double sleepMillis = Math.random() * MAX_SLEEP_MILLIS + 1;
                        System.out.println(String.format("next=%s thread=%s sleepMillis=%d", next, Thread.currentThread().getName(), sleepMillis.longValue()));
                        Thread.sleep(sleepMillis.longValue());
                    } catch (Exception e) {
                        System.err.println("Failed to sleep");
                    }
                })
                .subscribe(Acknowledgeable.consuming(string -> latch.countDown(), Acknowledgeable::acknowledge)));

        //Step 4) Produce random UUIDs to the topic we're processing above
        Flux.range(0, NUM_SAMPLES)
            .subscribeOn(Schedulers.boundedElastic())
            .map(i -> UUID.randomUUID())
            .map(UUID::toString)
            .transform(new KafkaValueSenderFactory<String>(kafkaSubscriberConfig).sendValues(TOPIC, Function.identity()))
            .subscribe();

        //Step 5) Await processing completion of the UUIDs we produced
        Instant begin = Instant.now();
        latch.await();

        System.out.println("Processing duration=" + Duration.between(begin, Instant.now()));
        System.exit(0);
    }

}
