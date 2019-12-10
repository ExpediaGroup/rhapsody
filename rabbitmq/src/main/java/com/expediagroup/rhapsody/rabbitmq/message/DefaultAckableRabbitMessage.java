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

import java.util.function.Consumer;

import com.expediagroup.rhapsody.api.AbstractAcknowledgeable;
import com.expediagroup.rhapsody.api.AcknowledgeableFactory;
import com.expediagroup.rhapsody.api.ComposedAcknowledgeable;

public class DefaultAckableRabbitMessage<T> extends AbstractAcknowledgeable<RabbitMessage<T>> implements AckableRabbitMessage<T> {

    private final RabbitMessage<T> rabbitMessage;

    private final Acker acker;

    private final Consumer<? super Throwable> nacknowledger;

    public DefaultAckableRabbitMessage(RabbitMessage<T> rabbitMessage, Acker acker, Consumer<? super Throwable> nacknowledger) {
        this.rabbitMessage = rabbitMessage;
        this.acker = acker;
        this.nacknowledger = nacknowledger;
    }

    @Override
    public RabbitMessage<T> get() {
        return rabbitMessage;
    }

    @Override
    public Runnable getAcknowledger() {
        return () -> acker.ack(Acker.AckType.SINGLE);
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return nacknowledger;
    }

    @Override
    public void multipleAck() {
        acker.ack(Acker.AckType.MULTIPLE);
    }

    @Override
    protected <R> AcknowledgeableFactory<R> createPropagator() {
        return ComposedAcknowledgeable::new;
    }
}
