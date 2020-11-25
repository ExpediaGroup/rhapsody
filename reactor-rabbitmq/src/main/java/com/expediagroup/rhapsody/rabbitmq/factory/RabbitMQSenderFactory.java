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
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.rabbitmq.message.RabbitMessage;
import com.expediagroup.rhapsody.rabbitmq.message.RabbitMessageSendInterceptor;
import com.expediagroup.rhapsody.rabbitmq.serde.BodySerializer;
import com.expediagroup.rhapsody.util.ConfigLoading;

import reactor.core.publisher.Flux;
import reactor.rabbitmq.CorrelableOutboundMessage;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.OutboundMessageResult;
import reactor.rabbitmq.SendOptions;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;
import reactor.util.retry.Retry;

public class RabbitMQSenderFactory<T> {

    public static final String RESUBSCRIBE_ON_ERROR_CONFIG = "resubscribe.on.error";

    private static final boolean DEFAULT_RESUBSCRIBE_ON_ERROR = true;

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQSenderFactory.class);

    private static final SendOptions DEFAULT_SEND_OPTIONS = new SendOptions();

    private static final SendOptions DEFAULT_ACKNOWLEDGEABLE_SEND_OPTIONS = new SendOptions()
        .exceptionHandler(RabbitMQSenderFactory::handleAcknowledgeableSendException);

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

    public Function<Publisher<Acknowledgeable<RabbitMessage<T>>>, Flux<Acknowledgeable<OutboundMessageResult<CorrelableOutboundMessage<T>>>>>
    sendAcknowledgeable() {
        return this::sendAcknowledgeable;
    }

    public Flux<Acknowledgeable<OutboundMessageResult<CorrelableOutboundMessage<T>>>>
    sendAcknowledgeable(Publisher<Acknowledgeable<RabbitMessage<T>>> acknowledgeables) {
        return Flux.from(acknowledgeables)
            .map(interceptors.isEmpty() ? Function.identity() : Acknowledgeable.mapping(this::applyInterceptors))
            .map(message -> serialize(message, Acknowledgeable::get, acknowledgeable -> acknowledgeable.map(RabbitMessage::getBody)))
            .transform(serializedMessages -> sendSerialized(serializedMessages, DEFAULT_ACKNOWLEDGEABLE_SEND_OPTIONS))
            .map(this::toAcknowledgeableResult);
    }

    public Function<Publisher<RabbitMessage<T>>, Flux<OutboundMessageResult<CorrelableOutboundMessage<T>>>> send() {
        return this::send;
    }

    public Flux<OutboundMessageResult<CorrelableOutboundMessage<T>>> send(Publisher<RabbitMessage<T>> messages) {
        return Flux.from(messages)
            .map(this::applyInterceptors)
            .map(message -> serialize(message, Function.identity(), RabbitMessage::getBody))
            .transform(serializedMessages -> sendSerialized(serializedMessages, DEFAULT_SEND_OPTIONS));
    }

    private RabbitMessage<T> applyInterceptors(RabbitMessage<T> rabbitMessage) {
        for (RabbitMessageSendInterceptor<T> interceptor : interceptors) {
            rabbitMessage = interceptor.onSend(rabbitMessage);
        }
        return rabbitMessage;
    }

    private <W, C> CorrelableOutboundMessage<C> serialize(W wrapper, Function<W, RabbitMessage<T>> messageExtractor, Function<W, C> correlationExtractor) {
        RabbitMessage<T> message = messageExtractor.apply(wrapper);
        byte[] serializedBody = bodySerializer.serialize(message.getBody());
        return new CorrelableOutboundMessage<>(
            message.getExchange(), message.getRoutingKey(), message.getProperties(), serializedBody, correlationExtractor.apply(wrapper));
    }

    private <M extends OutboundMessage> Flux<OutboundMessageResult<M>> sendSerialized(Flux<M> messages, SendOptions sendOptions) {
        return sender.sendWithTypedPublishConfirms(messages, sendOptions)
            .doOnError(error -> LOGGER.warn("An Error was encountered while trying to send to RabbitMQ. resubscribeOnError={}", resubscribeOnError, error))
            .retryWhen(Retry.indefinitely().filter(error -> resubscribeOnError));
    }

    private Acknowledgeable<OutboundMessageResult<CorrelableOutboundMessage<T>>>
    toAcknowledgeableResult(OutboundMessageResult<CorrelableOutboundMessage<Acknowledgeable<T>>> outboundMessageResult) {
        CorrelableOutboundMessage<Acknowledgeable<T>> outboundMessage = outboundMessageResult.getOutboundMessage();
        return outboundMessage.getCorrelationMetadata().map(data -> new OutboundMessageResult<>(
            toCorrelableOutboundMessage(outboundMessage, data), outboundMessageResult.isAck(), outboundMessageResult.isReturned()));
    }

    private CorrelableOutboundMessage<T> toCorrelableOutboundMessage(OutboundMessage message, T correlationMetadata) {
        return new CorrelableOutboundMessage<>(
            message.getExchange(), message.getRoutingKey(), message.getProperties(), message.getBody(), correlationMetadata);
    }

    //TODO This is an ugly result of SendContext not being parameterized on `sendWithTypedPublishConfirms`
    private static void handleAcknowledgeableSendException(Sender.SendContext sendContext, Exception error) {
        Acknowledgeable.class.cast(CorrelableOutboundMessage.class.cast(sendContext.getMessage()).getCorrelationMetadata()).nacknowledge(error);
    }
}
