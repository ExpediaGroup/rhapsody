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

import java.util.List;
import java.util.Map;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.rhapsody.rabbitmq.message.RabbitMessage;
import com.expediagroup.rhapsody.rabbitmq.message.RabbitMessageSendInterceptor;
import com.expediagroup.rhapsody.rabbitmq.serde.BodySerializer;
import com.expediagroup.rhapsody.util.ConfigLoading;

import reactor.core.publisher.Flux;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.OutboundMessageResult;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;

public class RabbitMQSenderFactory<T> {

    public static final String RESUBSCRIBE_ON_ERROR_CONFIG = "resubscribe.on.error";

    private static final boolean DEFAULT_RESUBSCRIBE_ON_ERROR = true;

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQSenderFactory.class);

    private final Sender sender;

    private final List<RabbitMessageSendInterceptor<T>> interceptors;

    private final BodySerializer<T> bodySerializer;

    private final boolean resubscribeOnError;

    public RabbitMQSenderFactory(RabbitConfigFactory configFactory) {
        Map<String, Object> properties = configFactory.create();
        this.sender = new Sender(new SenderOptions().connectionFactory(RabbitConfigFactory.createConnectionFactory(properties)));
        this.interceptors = RabbitMessageSendInterceptor.createInterceptors(properties);
        this.bodySerializer = BodySerializer.create(properties);
        this.resubscribeOnError = ConfigLoading.load(properties, RESUBSCRIBE_ON_ERROR_CONFIG, Boolean::valueOf, DEFAULT_RESUBSCRIBE_ON_ERROR);
    }

    public Flux<OutboundMessageResult> send(Publisher<RabbitMessage<T>> messages) {
        return Flux.from(messages)
            .map(this::applyInterceptors)
            .map(this::serialize)
            .transform(this::sendSerialized);
    }

    private RabbitMessage<T> applyInterceptors(RabbitMessage<T> rabbitMessage) {
        for (RabbitMessageSendInterceptor<T> interceptor : interceptors) {
            rabbitMessage = interceptor.onSend(rabbitMessage);
        }
        return rabbitMessage;
    }

    private OutboundMessage serialize(RabbitMessage<T> message) {
        byte[] serializedBody = bodySerializer.serialize(message.getBody());
        return new OutboundMessage(message.getExchange(), message.getRoutingKey(), message.getProperties(), serializedBody);
    }

    private Flux<OutboundMessageResult> sendSerialized(Flux<OutboundMessage> messages) {
        return messages
            .transform(sender::sendWithPublishConfirms)
            .doOnError(error -> LOGGER.warn("An Error was encountered while trying to send to RabbitMQ. resubscribeOnError={}", resubscribeOnError, error))
            .retry(error -> resubscribeOnError);
    }
}
