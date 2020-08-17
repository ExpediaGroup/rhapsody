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
package com.expediagroup.rhapsody.samples.errorhandling;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.core.transformer.ResubscribingTransformer;
import com.expediagroup.rhapsody.core.transformer.ResubscriptionConfig;
import com.expediagroup.rhapsody.kafka.acknowledgement.OrderManagingReceiverAcknowledgementStrategy;
import com.expediagroup.rhapsody.kafka.factory.KafkaConfigFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueFluxFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueSenderFactory;
import com.expediagroup.rhapsody.kafka.sending.AcknowledgingLoggingSenderSubscriber;
import com.expediagroup.rhapsody.kafka.test.TestKafkaFactory;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * This sample demonstrates how to apply resiliency characteristics to Kafka streaming processes
 * via "resubscription". When either an upstream error or Acknowledgeable downstream error occurs,
 * an error will be emitted in the Flux produced by the Kafka Flux Factory. This causes the
 * following sequence of events:
 * 1) Cancellation of the first Flux of values from the Kafka Flux Factory
 * 2) Commitment of acknowledged offsets
 * 3) Closure of the underlying Kafka Consumer
 * 4) Resubscription, resulting in a new Kafka Consumer and new Flux
 * 5) Reconsumption of records whose offsets have not yet been committed
 * This sample also illustrates how we ensure that any given Record's offset is not committed until
 * after all records in the same topic-partition that come before it have been acknowledged. This
 * is a core behavior of Rhapsody's at-least-once guarantee
 */
public class KafkaErrorHandling {

    private static final Map<String, ?> TEST_KAFKA_CONFIG = new TestKafkaFactory().createKafka();

    private static final String TOPIC_1 = "TOPIC_1";

    private static final String TOPIC_2 = "TOPIC_2";

    private static final Scheduler SCHEDULER = Schedulers.newElastic(KafkaErrorHandling.class.getSimpleName());

    public static void main(String[] args) throws Exception {
        //Step 1) Create Kafka Producer Config for Producer that backs Sender's Subscriber
        //implementation
        KafkaConfigFactory kafkaSubscriberConfig = new KafkaConfigFactory();
        kafkaSubscriberConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaSubscriberConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaErrorHandling.class.getSimpleName());
        kafkaSubscriberConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaSubscriberConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        //Step 2) Create "faulty" Kafka Producer Config where the second value we try to process
        //will throw an Exception at serialization time
        KafkaConfigFactory faultyKafkaSubscriberConfig = new KafkaConfigFactory();
        faultyKafkaSubscriberConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        faultyKafkaSubscriberConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaErrorHandling.class.getSimpleName());
        faultyKafkaSubscriberConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        faultyKafkaSubscriberConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, SecondTimeFailingSerializer.class.getName());
        faultyKafkaSubscriberConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        faultyKafkaSubscriberConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        //Step 3) Create Kafka Consumer Config for Consumer that backs Receiver's Publisher
        //implementation
        KafkaConfigFactory kafkaPublisherConfig = new KafkaConfigFactory();
        kafkaPublisherConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaPublisherConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaErrorHandling.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaErrorHandling.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaPublisherConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaPublisherConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //Step 4) Apply stream processing to the Kafka topic we'll produce records to. The
        //"processing" in this case is contrived to block a particular record, "TEST_1", from
        //proceeding while a record that comes strictly after it, "TEST_2", has finished
        //processing. At that point, "TEST_1" will fail due to our "faulty" configuration where
        //serialization will fail the second time it's executed. This results in a "negative
        //acknowledgement" which causes an upstream error to be emitted. Coupled with the
        //"resubscription" we have added to this process, the end result is that we re-consume
        //BOTH records and successfully process them
        CountDownLatch latch = new CountDownLatch(1);
        List<String> successfullyProcessed = new CopyOnWriteArrayList<>();
        KafkaValueSenderFactory<String> senderFactory = new KafkaValueSenderFactory<>(faultyKafkaSubscriberConfig);
        new KafkaValueFluxFactory<String>(kafkaPublisherConfig)
            .receiveValue(Collections.singletonList(TOPIC_1), new OrderManagingReceiverAcknowledgementStrategy())
            .transform(new ResubscribingTransformer<>(new ResubscriptionConfig(Duration.ofSeconds(2))))
            .groupBy(Acknowledgeable::get)
            .subscribe(groupFlux -> groupFlux
                .publishOn(SCHEDULER)
                .map(Acknowledgeable.mapping(String::toUpperCase))
                .doOnNext(next -> {
                    try {
                        if (next.get().equals("TEST_1")) {
                            latch.await();
                        }
                    } catch (Exception e) {
                        System.err.println("Unexpected failure=" + e);
                    }
                })
                .transform(senderFactory.sendAcknowledgeableValues(TOPIC_2, Function.identity()))
                .doOnNext(next -> latch.countDown())
                .doOnNext(senderResult -> {
                    if (senderResult.get().exception() == null) {
                        successfullyProcessed.add(senderResult.get().correlationMetadata());
                    }
                })
                .subscribe(new AcknowledgingLoggingSenderSubscriber<>()));

        //Step 5) Produce the records to be consumed above. Note that we are using the same record
        //key for each data item, which will cause these items to show up in the order we emit them
        //on the same topic-partition
        Flux.just("test_1", "test_2")
            .subscribeOn(Schedulers.elastic())
            .transform(new KafkaValueSenderFactory<String>(kafkaSubscriberConfig).sendValues(TOPIC_1, string -> "KEY"))
            .subscribe();

        //Step 6) Await the successful completion of the data we emitted. There should be exactly
        //three successfully processed elements, since we end up successfully processing "test_2"
        //twice
        while (true) {
            if (successfullyProcessed.size() >= 3) {
                break;
            }
            Thread.sleep(100L);
        }

        System.out.println("processed: " + successfullyProcessed);
        System.exit(0);
    }

    public static class SecondTimeFailingSerializer extends StringSerializer {

        private static final AtomicInteger COUNT = new AtomicInteger();

        @Override
        public byte[] serialize(String topic, String data) {
            if (COUNT.incrementAndGet() == 2) {
                throw new IllegalStateException("This serializer will fail the second time it's used");
            }
            return super.serialize(topic, data);
        }
    }
}
