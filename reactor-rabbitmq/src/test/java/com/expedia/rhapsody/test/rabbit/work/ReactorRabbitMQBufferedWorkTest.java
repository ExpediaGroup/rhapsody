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
package com.expedia.rhapsody.test.rabbit.work;

import java.util.Map;
import java.util.function.Consumer;

import org.junit.BeforeClass;
import org.reactivestreams.Publisher;

import com.expedia.rhapsody.amqp.test.TestAmqpFactory;
import com.expedia.rhapsody.core.adapter.Adapters;
import com.expedia.rhapsody.core.transformer.AutoAcknowledgementConfig;
import com.expedia.rhapsody.rabbitmq.factory.RabbitConfigFactory;
import com.expedia.rhapsody.rabbitmq.factory.RabbitMQBodyFluxFactory;
import com.expedia.rhapsody.rabbitmq.factory.RabbitMQBodySenderFactory;
import com.expedia.rhapsody.rabbitmq.message.DefaultRabbitMessageCreator;
import com.expedia.rhapsody.rabbitmq.message.RabbitMessageCreator;
import com.expedia.rhapsody.test.TestWork;
import com.expedia.rhapsody.test.core.work.AbstractBufferedWorkTest;
import com.expedia.rhapsody.test.rabbit.factory.TestRabbitConfigFactory;
import com.rabbitmq.client.Channel;

import reactor.core.publisher.Flux;

public class ReactorRabbitMQBufferedWorkTest extends AbstractBufferedWorkTest {

    private static final Map<String, ?> AMQP_CONFIG = new TestAmqpFactory().createAmqp();

    public ReactorRabbitMQBufferedWorkTest() {
        this(TestRabbitConfigFactory.createJacksonFactory(AMQP_CONFIG, TestWork.class));
    }

    protected ReactorRabbitMQBufferedWorkTest(RabbitConfigFactory configFactory) {
        super(() -> Adapters.toSendingSubscriber(createSender(configFactory, TestWork.class.getSimpleName())),
            () -> createPublisher(configFactory, TestWork.class.getSimpleName()));
    }

    @BeforeClass
    public static void setupTest() throws Exception {
        Channel channel = TestRabbitConfigFactory.createFactory(AMQP_CONFIG).createConnectionFactory().newConnection().createChannel();
        channel.queueDeclare(TestWork.class.getSimpleName(), false, false, false, null);
    }

    private static Consumer<Publisher<TestWork>> createSender(RabbitConfigFactory configFactory, String queue) {
        RabbitMessageCreator<TestWork> messageCreator = DefaultRabbitMessageCreator.persistentBasicToDefaultExchange(queue);
        return bodies -> new RabbitMQBodySenderFactory<TestWork>(configFactory)
            .sendBodies(bodies, messageCreator)
            .subscribe();
    }

    private static Flux<TestWork> createPublisher(RabbitConfigFactory configFactory, String queue) {
        return new RabbitMQBodyFluxFactory<TestWork>(configFactory).consumeAutoBody(queue, new AutoAcknowledgementConfig());
    }
}
