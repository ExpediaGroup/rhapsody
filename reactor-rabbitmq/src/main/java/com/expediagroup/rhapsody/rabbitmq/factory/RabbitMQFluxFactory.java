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
package com.expediagroup.rhapsody.rabbitmq.factory;

import java.util.Map;
import java.util.function.Consumer;

import com.expediagroup.rhapsody.core.transformer.AutoAcknowledgementConfig;
import com.expediagroup.rhapsody.core.transformer.AutoAcknowledgingTransformer;
import com.expediagroup.rhapsody.rabbitmq.message.AckableRabbitMessage;
import com.expediagroup.rhapsody.rabbitmq.message.AckerNacker;
import com.expediagroup.rhapsody.rabbitmq.message.RabbitMessage;
import com.expediagroup.rhapsody.rabbitmq.message.SafeAckerNacker;
import com.expediagroup.rhapsody.rabbitmq.serde.BodyDeserializer;
import com.expediagroup.rhapsody.util.ConfigLoading;
import com.rabbitmq.client.ConnectionFactory;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.rabbitmq.AcknowledgableDelivery;
import reactor.rabbitmq.ConsumeOptions;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;

public class RabbitMQFluxFactory<T> {

    public static final String QOS_CONFIG = "qos";

    private final Map<String, Object> properties;

    private final NacknowledgerFactory nacknowledgerFactory;

    private final ConnectionFactory connectionFactory;

    private final BodyDeserializer<T> bodyDeserializer;

    private final AckableRabbitMessageFactory<T> messageFactory;

    public RabbitMQFluxFactory(RabbitConfigFactory configFactory) {
        this(configFactory, new DiscardSingleNacknowledgerFactory());
    }

    public RabbitMQFluxFactory(RabbitConfigFactory configFactory, NacknowledgerFactory nacknowledgerFactory) {
        this.properties = configFactory.create();
        this.nacknowledgerFactory = nacknowledgerFactory;
        this.connectionFactory = RabbitConfigFactory.createConnectionFactory(properties);
        this.bodyDeserializer = BodyDeserializer.create(properties);
        this.messageFactory = AckableRabbitMessageFactory.create(properties);
    }

    public Flux<RabbitMessage<T>> consumeAuto(String queue, AutoAcknowledgementConfig autoAcknowledgementConfig) {
        return consume(queue)
            .transformDeferred(new AutoAcknowledgingTransformer<>(autoAcknowledgementConfig, flux -> flux.takeLast(1), AckableRabbitMessage::multipleAck))
            .map(AckableRabbitMessage::get);
    }

    public Flux<AckableRabbitMessage<T>> consume(String queue) {
        return Flux.defer(() -> consumeManualAck(queue));
    }

    private Flux<AckableRabbitMessage<T>> consumeManualAck(String queue) {
        FluxProcessor<AckableRabbitMessage<T>, AckableRabbitMessage<T>> manualProcessor = EmitterProcessor.create();
        FluxSink<AckableRabbitMessage<T>> sink = manualProcessor.sink();
        return new Receiver(createReceiverOptions())
            .consumeManualAck(queue, createConsumeOptions())
            .map(delivery -> deserialize(delivery, sink::error))
            .mergeWith(manualProcessor);
    }

    private ReceiverOptions createReceiverOptions() {
        ReceiverOptions receiverOptions = new ReceiverOptions();
        receiverOptions.connectionFactory(connectionFactory);
        return receiverOptions;
    }

    private ConsumeOptions createConsumeOptions() {
        ConsumeOptions consumeOptions = new ConsumeOptions();
        consumeOptions.qos(ConfigLoading.load(properties, QOS_CONFIG, Integer::valueOf, consumeOptions.getQos()));
        return consumeOptions;
    }

    private AckableRabbitMessage<T> deserialize(AcknowledgableDelivery delivery, Consumer<? super Throwable> errorEmitter) {
        RabbitMessage<T> rabbitMessage = new RabbitMessage<>(
            delivery.getEnvelope().getExchange(),
            delivery.getEnvelope().getRoutingKey(),
            delivery.getProperties(),
            bodyDeserializer.deserialize(delivery.getBody()));

        AckerNacker ackerNacker = new SafeAckerNacker(
            ackType -> delivery.ack(ackType.isMultiple()),
            nackType -> delivery.nack(nackType.isMultiple(), nackType.isRequeue()),
            errorEmitter);

        return messageFactory.create(rabbitMessage, ackerNacker, nacknowledgerFactory.create(delivery, ackerNacker, errorEmitter));
    }
}
