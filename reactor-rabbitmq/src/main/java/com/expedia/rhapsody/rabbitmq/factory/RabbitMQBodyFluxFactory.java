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
package com.expedia.rhapsody.rabbitmq.factory;

import com.expedia.rhapsody.api.Acknowledgeable;
import com.expedia.rhapsody.core.transformer.AutoAcknowledgementConfig;
import com.expedia.rhapsody.rabbitmq.message.RabbitMessage;

import reactor.core.publisher.Flux;

public class RabbitMQBodyFluxFactory<T> extends RabbitMQFluxFactory<T> {

    public RabbitMQBodyFluxFactory(RabbitConfigFactory configFactory) {
        super(configFactory);
    }

    public RabbitMQBodyFluxFactory(RabbitConfigFactory configFactory, NacknowledgerFactory nacknowledgerFactory) {
        super(configFactory, nacknowledgerFactory);
    }

    public Flux<T> consumeAutoBody(String queue, AutoAcknowledgementConfig autoAcknowledgementConfig) {
        return consumeAuto(queue, autoAcknowledgementConfig)
            .filter(rabbitMessage -> rabbitMessage.getBody() != null)
            .map(RabbitMessage::getBody);
    }

    public Flux<Acknowledgeable<T>> consumeBody(String queue) {
        return consume(queue)
            .filter(Acknowledgeable.filtering(rabbitMessage -> rabbitMessage.getBody() != null, Acknowledgeable::acknowledge))
            .map(Acknowledgeable.mapping(RabbitMessage::getBody));
    }
}
