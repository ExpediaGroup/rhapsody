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
package com.expediagroup.rhapsody.samples.deduplication;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.api.IdentityDeduplication;
import com.expediagroup.rhapsody.core.transformer.DeduplicatingTransformer;
import com.expediagroup.rhapsody.core.transformer.DeduplicationConfig;
import com.expediagroup.rhapsody.kafka.acknowledgement.OrderManagingReceiverAcknowledgementStrategy;
import com.expediagroup.rhapsody.kafka.factory.KafkaConfigFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaFluxFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueFluxFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueSenderFactory;
import com.expediagroup.rhapsody.kafka.test.TestKafkaFactory;
import com.expediagroup.rhapsody.util.Defaults;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderResult;

/**
 * This sample shows how message consumption may be de-duped via {@link DeduplicatingTransformer}.
 * Note that this example continues to incorporate {@link Acknowledgeable acknowledgement}
 * propagation such that at-least-once guarantee is maintained in the face of "aggregation", or
 * "many to one" processing transformations.
 */
public class KafkaDeduplication {

    private static final Map<String, ?> TEST_KAFKA_CONFIG = new TestKafkaFactory().createKafka();

    private static final String TOPIC_1 = "TOPIC_1";

    private static final String TOPIC_2 = "TOPIC_2";

    public static void main(String[] args) throws Exception {
        //Step 1) Create Kafka Producer Config for Producer that backs Sender's Subscriber
        //implementation
        KafkaConfigFactory kafkaSubscriberConfig = new KafkaConfigFactory();
        kafkaSubscriberConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaSubscriberConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaDeduplication.class.getSimpleName());
        kafkaSubscriberConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaSubscriberConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        //Step 2) Create Kafka Consumer Config for Consumer that backs Receiver's Publisher
        //implementation. Note that we block our main Thread on partition assignment such that
        //subsequently produced Records are processed
        KafkaConfigFactory kafkaPublisherConfig = new KafkaConfigFactory();
        kafkaPublisherConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaPublisherConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaDeduplication.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaDeduplication.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaPublisherConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaPublisherConfig.put(KafkaFluxFactory.BLOCK_REQUEST_ON_PARTITION_ASSIGNMENT_CONFIG, true);

        //Step 3) Create a deduplication config where, upon the first occurrence of an item, a
        //window with a maximum timespan of 4 seconds and a max size of 4 items is opened. We also
        //indicate a default for the max number of concurrent windows allowed to be opened at a
        //time
        DeduplicationConfig deduplicationConfig = new DeduplicationConfig(Duration.ofSeconds(2), 4L, Defaults.CONCURRENCY);

        //Step 4) Apply consumption of the Kafka topic we've produced data to as a stream process.
        //The "process" in this stream upper-cases the values we sent previously, producing the
        //result to another topic. This portion also adheres to the responsibilities obliged by the
        //consumption of Acknowledgeable data. Note that we again need to explicitly limit the
        //number of results we expect ('.take(4)'), or else this Flow would never complete.
        //Deduplication is applied early in the stream to limit the processing of duplicate methods
        //via DeduplicatingTransformer
        KafkaValueSenderFactory<String> senderFactory = new KafkaValueSenderFactory<>(kafkaSubscriberConfig);
        Mono<List<String>> processed = new KafkaValueFluxFactory<String>(kafkaPublisherConfig)
            .receiveValue(Collections.singletonList(TOPIC_1), new OrderManagingReceiverAcknowledgementStrategy())
            .transform(DeduplicatingTransformer.acknowledgeable(deduplicationConfig, new IdentityDeduplication<>()))
            .map(Acknowledgeable.mapping(String::toUpperCase))
            .transform(upperCasedValues -> senderFactory.sendAcknowledgeableValues(upperCasedValues, value -> TOPIC_2, Function.identity()))
            .doOnNext(Acknowledgeable::acknowledge)
            .map(Acknowledgeable::get)
            .map(SenderResult::correlationMetadata)
            .doOnNext(next -> System.out.println("Processed next=" + next))
            .switchOnFirst((signal, flux) -> signal.hasValue() ? flux.take(Duration.ofSeconds(5)) : flux)
            .cache()
            .publish()
            .autoConnect(0)
            .collectList();

        //Step 5) Create the List of values we'll emit to the topic. These values will be
        //immediately emitted such that the above stream process will see a near immediate
        //consumption of the following counts of data:
        //"ONE" - 2 times
        //"TWO" - 6 times
        //"THREE" - 1 time
        //Due to our deduplication config, we should see two processing occurrences of "TWO" where
        //the first executes near-immediately, followed by the second that is processed around
        //two seconds later with singular occurrences of "ONE" and "THREE"
        List<String> values = Arrays.asList("TWO", "TWO", "ONE", "TWO", "THREE", "TWO", "ONE", "TWO", "TWO");

        //Step 6) Send the above values to the Kafka topic we're processing
        new KafkaValueSenderFactory<>(kafkaSubscriberConfig)
            .sendValues(Flux.fromIterable(values), value -> TOPIC_1, Function.identity())
            .subscribe();

        //Step 7) Await consumption of the results
        List<String> result = processed.block();
        System.out.println("result=" + result);

        System.exit(0);
    }
}
