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
package com.expediagroup.rhapsody.test.rabbit.acknowledgeable;

import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import org.junit.Before;
import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.amqp.test.TestAmqpFactory;
import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.core.adapter.Adapters;
import com.expediagroup.rhapsody.rabbitmq.factory.RabbitConfigFactory;
import com.expediagroup.rhapsody.rabbitmq.factory.RabbitMQBodyFluxFactory;
import com.expediagroup.rhapsody.rabbitmq.factory.RabbitMQBodySenderFactory;
import com.expediagroup.rhapsody.rabbitmq.factory.RequeueSingleNacknowledgerFactory;
import com.expediagroup.rhapsody.rabbitmq.message.DefaultRabbitMessageCreator;
import com.expediagroup.rhapsody.rabbitmq.message.RabbitMessageCreator;
import com.expediagroup.rhapsody.test.core.acknowledgeable.AbstractAcknowledgeablePublishingTest;
import com.expediagroup.rhapsody.test.rabbit.factory.TestRabbitConfigFactory;
import com.rabbitmq.client.Channel;

import reactor.core.publisher.Flux;

public class ReactorRabbitMQAcknowledgeablePublishingTest extends AbstractAcknowledgeablePublishingTest {

    private static final Map<String, ?> AMQP_CONFIG = new TestAmqpFactory().createAmqp();

    private final String queue;

    public ReactorRabbitMQAcknowledgeablePublishingTest() {
        this(TestRabbitConfigFactory.createJacksonFactory(AMQP_CONFIG, String.class),
            Acknowledgeable.class.getSimpleName() + String.class.getSimpleName() + UUID.randomUUID());
    }

    protected ReactorRabbitMQAcknowledgeablePublishingTest(RabbitConfigFactory configFactory, String queue) {
        super(() -> Adapters.toSendingSubscriber(createSender(configFactory, queue)),
            () -> createPublisher(configFactory, queue));
        this.queue = queue;
    }

    @Before
    public void setup() throws Exception {
        Channel channel = TestRabbitConfigFactory.createFactory(AMQP_CONFIG).createConnectionFactory().newConnection().createChannel();
        channel.queueDeclare(queue, false, false, false, null);
    }

    private static Consumer<Publisher<String>> createSender(RabbitConfigFactory configFactory, String queue) {
        RabbitMessageCreator<String> messageCreator = DefaultRabbitMessageCreator.persistentBasicToDefaultExchange(queue);
        return bodies -> new RabbitMQBodySenderFactory<String>(configFactory)
            .sendBodies(bodies, messageCreator)
            .subscribe();
    }

    private static Flux<Acknowledgeable<String>> createPublisher(RabbitConfigFactory configFactory, String queue) {
        return new RabbitMQBodyFluxFactory<String>(configFactory, new RequeueSingleNacknowledgerFactory()).consumeBody(queue);
    }
}
