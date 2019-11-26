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
package com.expedia.rhapsody.rabbitmq.message;

import java.util.Objects;
import java.util.function.Function;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.MessageProperties;

public class DefaultRabbitMessageCreator<T> extends AbstractRabbitMessageCreator<T> {

    private final Function<T, ?> exchangeExtractor;

    private final Function<T, ?> routingKeyExtractor;

    public DefaultRabbitMessageCreator(AMQP.BasicProperties initialProperties, Function<T, ?> exchangeExtractor, Function<T, ?> routingKeyExtractor) {
        super(initialProperties);
        this.exchangeExtractor = exchangeExtractor;
        this.routingKeyExtractor = routingKeyExtractor;
    }

    public static <T> DefaultRabbitMessageCreator<T> persistentBasicToDefaultExchange(String queue) {
        return new DefaultRabbitMessageCreator<>(MessageProperties.PERSISTENT_BASIC, t -> "", t -> queue);
    }

    public static <T> DefaultRabbitMessageCreator<T> persistentBasicToDefaultExchange(Function<T, ?> queueExtractor) {
        return new DefaultRabbitMessageCreator<>(MessageProperties.PERSISTENT_BASIC, t -> "", queueExtractor);
    }

    public static <T> DefaultRabbitMessageCreator<T> persistentBasicToExchange(String exchange, String routingKey) {
        return new DefaultRabbitMessageCreator<>(MessageProperties.PERSISTENT_BASIC, t -> exchange, t -> routingKey);
    }

    public static <T> DefaultRabbitMessageCreator<T> persistentBasicToExchange(String exchange, Function<T, ?> routingKeyExtractor) {
        return new DefaultRabbitMessageCreator<>(MessageProperties.PERSISTENT_BASIC, t -> exchange, routingKeyExtractor);
    }

    public static <T> DefaultRabbitMessageCreator<T> toDefaultExchange(AMQP.BasicProperties properties, String queue) {
        return new DefaultRabbitMessageCreator<>(properties, t -> "", t -> queue);
    }

    public static <T> DefaultRabbitMessageCreator<T> toDefaultExchange(AMQP.BasicProperties properties, Function<T, ?> queueExtractor) {
        return new DefaultRabbitMessageCreator<>(properties, t -> "", queueExtractor);
    }

    public static <T> DefaultRabbitMessageCreator<T> toExchange(AMQP.BasicProperties properties, String exchange, String routingKey) {
        return new DefaultRabbitMessageCreator<>(properties, t -> exchange, t -> routingKey);
    }

    public static <T> DefaultRabbitMessageCreator<T> toExchange(AMQP.BasicProperties properties, String exchange, Function<T, ?> routingKeyExtractor) {
        return new DefaultRabbitMessageCreator<>(properties, t -> exchange, routingKeyExtractor);
    }

    @Override
    protected String extractExchange(T t) {
        return Objects.toString(exchangeExtractor.apply(t));
    }

    @Override
    protected String extractRoutingKey(T t) {
        return Objects.toString(routingKeyExtractor.apply(t));
    }
}
