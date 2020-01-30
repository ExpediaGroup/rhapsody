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
package com.expediagroup.rhapsody.samples.endtoendtoend;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.expediagroup.rhapsody.core.transformer.AutoAcknowledgementConfig;
import com.expediagroup.rhapsody.kafka.factory.KafkaConfigFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueFluxFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueSenderFactory;
import com.expediagroup.rhapsody.kafka.test.TestKafkaFactory;

import reactor.core.publisher.Flux;

/**
 * Part 2 of this sample set illustrates how to both reactively produce and consume records from a
 * Kafka topic
 */
public class KafkaPart2 {

    private static final Map<String, ?> TEST_KAFKA_CONFIG = new TestKafkaFactory().createKafka();

    private static final String TOPIC = "TOPIC";

    public static void main(String[] args) throws Exception {
        //Step 1) Create Kafka Producer Config for Producer that backs Sender's Subscriber
        //implementation
        KafkaConfigFactory kafkaSubscriberConfig = new KafkaConfigFactory();
        kafkaSubscriberConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaSubscriberConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaPart2.class.getSimpleName());
        kafkaSubscriberConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaSubscriberConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        //Step 2) Create Kafka Consumer Config for Consumer that backs Receiver's Publisher
        //implementation. Note that we use an Auto Offset Reset of 'earliest' to ensure we receive
        //Records produced before subscribing with our new consumer group
        KafkaConfigFactory kafkaPublisherConfig = new KafkaConfigFactory();
        kafkaPublisherConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaPublisherConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaPart2.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaPart2.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaPublisherConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaPublisherConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //Step 3) Send some Record values to a hardcoded topic, using values as Record keys
        new KafkaValueSenderFactory<String>(kafkaSubscriberConfig)
            .sendValues(Flux.just("Test"), value -> TOPIC, Function.identity())
            .collectList()
            .doOnNext(senderResults -> System.out.println("senderResults: " + senderResults))
            .block();

        //Step 4) Subscribe to the same topic we produced previous values to. Note we are using
        //periodic acknowledgement of received Records. Note that we must specify how many records
        //to expect ('.take(1)'), or else this Flow would never complete
        new KafkaValueFluxFactory<String>(kafkaPublisherConfig)
            .receiveAutoValue(Collections.singletonList(TOPIC), new AutoAcknowledgementConfig())
            .take(1)
            .collectList()
            .doOnNext(receivedValues -> System.out.println("receivedValues: " + receivedValues))
            .block();

        System.exit(0);
    }
}
