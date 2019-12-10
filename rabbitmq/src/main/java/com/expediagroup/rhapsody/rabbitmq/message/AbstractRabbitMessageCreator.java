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
package com.expediagroup.rhapsody.rabbitmq.message;

import java.util.UUID;

import com.rabbitmq.client.AMQP;

public abstract class AbstractRabbitMessageCreator<T> implements RabbitMessageCreator<T> {

    private final AMQP.BasicProperties initialProperties;

    public AbstractRabbitMessageCreator(AMQP.BasicProperties initialProperties) {
        this.initialProperties = initialProperties;
    }

    @Override
    public RabbitMessage<T> apply(T t) {
        return new RabbitMessage<>(extractExchange(t), extractRoutingKey(t), createMessagePropertiesBuilder(t).build(), t);
    }

    protected abstract String extractExchange(T t);

    protected abstract String extractRoutingKey(T t);

    protected AMQP.BasicProperties.Builder createMessagePropertiesBuilder(T t) {
        return initialProperties.builder().messageId(UUID.randomUUID().toString());
    }
}
