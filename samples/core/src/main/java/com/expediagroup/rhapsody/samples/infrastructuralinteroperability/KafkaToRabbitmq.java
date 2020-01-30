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
package com.expediagroup.rhapsody.samples.infrastructuralinteroperability;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.expediagroup.rhapsody.amqp.test.TestAmqpFactory;
import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.kafka.acknowledgement.OrderManagingReceiverAcknowledgementStrategy;
import com.expediagroup.rhapsody.kafka.factory.KafkaConfigFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueFluxFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueSenderFactory;
import com.expediagroup.rhapsody.kafka.test.TestKafkaFactory;
import com.expediagroup.rhapsody.rabbitmq.factory.RabbitConfigFactory;
import com.expediagroup.rhapsody.rabbitmq.factory.RabbitMQBodyFluxFactory;
import com.expediagroup.rhapsody.rabbitmq.factory.RabbitMQBodySenderFactory;
import com.expediagroup.rhapsody.rabbitmq.message.DefaultRabbitMessageCreator;
import com.expediagroup.rhapsody.rabbitmq.serde.BodyDeserializer;
import com.expediagroup.rhapsody.rabbitmq.serde.BodySerializer;
import com.expediagroup.rhapsody.rabbitmq.serde.JacksonBodyDeserializer;
import com.expediagroup.rhapsody.rabbitmq.serde.JacksonBodySerializer;
import com.rabbitmq.client.Channel;

import reactor.core.publisher.Flux;

/**
 * This sample shows how an upstream Kafka Topic can be processed to a downstream RabbitMQ Queue.
 */
public class KafkaToRabbitmq {

    private static final Map<String, ?> TEST_KAFKA_CONFIG = new TestKafkaFactory().createKafka();

    private static final Map<String, ?> TEST_AMQP_CONFIG = new TestAmqpFactory().createAmqp();

    private static final String TOPIC = "TOPIC";

    private static final String QUEUE = "QUEUE";

    public static void main(String[] args) throws Exception {
        //Step 1) Create Kafka Producer Config for Producer that backs Sender's Subscriber
        //implementation
        KafkaConfigFactory kafkaSubscriberConfig = new KafkaConfigFactory();
        kafkaSubscriberConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaSubscriberConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaToRabbitmq.class.getSimpleName());
        kafkaSubscriberConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaSubscriberConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        //Step 2) Create Kafka Consumer Config for Consumer that backs Receiver's Publisher
        //implementation. Note that we use an Auto Offset Reset of 'earliest' to ensure we receive
        //Records produced before subscribing with our new consumer group
        KafkaConfigFactory kafkaPublisherConfig = new KafkaConfigFactory();
        kafkaPublisherConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaPublisherConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaToRabbitmq.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaToRabbitmq.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaPublisherConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaPublisherConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        //Step 3) Create a RabbitMQ Config that we'll use for Publishing and Subscribing
        RabbitConfigFactory rabbitConfigFactory = new RabbitConfigFactory();
        rabbitConfigFactory.put(RabbitConfigFactory.HOST_PROPERTY, TEST_AMQP_CONFIG.get(TestAmqpFactory.HOST_PROPERTY));
        rabbitConfigFactory.put(RabbitConfigFactory.PORT_PROPERTY, TEST_AMQP_CONFIG.get(TestAmqpFactory.PORT_PROPERTY));
        rabbitConfigFactory.put(RabbitConfigFactory.VIRTUAL_HOST_PROPERTY, TEST_AMQP_CONFIG.get(TestAmqpFactory.VIRTUAL_HOST_PROPERTY));
        rabbitConfigFactory.put(RabbitConfigFactory.USERNAME_PROPERTY, TEST_AMQP_CONFIG.get(TestAmqpFactory.USERNAME_PROPERTY));
        rabbitConfigFactory.put(RabbitConfigFactory.PASSWORD_PROPERTY, TEST_AMQP_CONFIG.get(TestAmqpFactory.PASSWORD_PROPERTY));
        rabbitConfigFactory.put(RabbitConfigFactory.SSL_PROPERTY, TEST_AMQP_CONFIG.get(TestAmqpFactory.SSL_PROPERTY));
        rabbitConfigFactory.put(BodySerializer.PROPERTY, JacksonBodySerializer.class.getName());
        rabbitConfigFactory.put(BodyDeserializer.PROPERTY, JacksonBodyDeserializer.class.getName());
        rabbitConfigFactory.put(JacksonBodyDeserializer.BODY_DESERIALIZATION_PROPERTY, String.class.getName());

        //Step 4) Producing to RabbitMQ requires that we declare a Queue to serve as the destination
        //and source of messages that we want to send/receive
        Channel channel = rabbitConfigFactory.createConnectionFactory().newConnection().createChannel();
        channel.queueDeclare(QUEUE, false, false, false, null);

        //Step 5) Produce some records to our Kafka topic
        new KafkaValueSenderFactory<String>(kafkaSubscriberConfig)
            .sendValues(Flux.just("Test"), value -> TOPIC, Function.identity())
            .collectList()
            .doOnNext(senderResults -> System.out.println("senderResults: " + senderResults))
            .block();

        //Step 6) Apply a streaming process over a Kafka -> RabbitMQ pairing
        new KafkaValueFluxFactory<String>(kafkaPublisherConfig)
            .receiveValue(Collections.singletonList(TOPIC), new OrderManagingReceiverAcknowledgementStrategy())
            .map(Acknowledgeable.mapping(String::toUpperCase))
            .transform(upperCasedValues -> new RabbitMQBodySenderFactory<String>(rabbitConfigFactory)
                .sendAcknowledgeableBodies(upperCasedValues, DefaultRabbitMessageCreator.persistentBasicToDefaultExchange(QUEUE)))
            .doOnNext(Acknowledgeable::acknowledge)
            .map(Acknowledgeable::get)
            .take(1)
            .collectList()
            .doOnNext(processedMessageResults -> System.out.println("processedMessageResults: " + processedMessageResults))
            .block();

        //Step 7) Consume the downstream results of the messages we processed
        new RabbitMQBodyFluxFactory<String>(rabbitConfigFactory)
            .consumeBody(QUEUE)
            .doOnNext(Acknowledgeable::acknowledge)
            .map(Acknowledgeable::get)
            .take(1)
            .collectList()
            .doOnNext(downstreamResults -> System.out.println("downstreamResults: " + downstreamResults))
            .block();

        System.exit(0);
    }
}
