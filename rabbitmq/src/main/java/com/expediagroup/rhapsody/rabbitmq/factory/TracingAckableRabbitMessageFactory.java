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

import java.util.function.Consumer;

import com.expediagroup.rhapsody.core.tracing.TracingDecoration;
import com.expediagroup.rhapsody.rabbitmq.message.AckableRabbitMessage;
import com.expediagroup.rhapsody.rabbitmq.message.Acker;
import com.expediagroup.rhapsody.rabbitmq.message.ConfigurableRabbitMessageTracing;
import com.expediagroup.rhapsody.rabbitmq.message.RabbitMessage;
import com.expediagroup.rhapsody.rabbitmq.message.TracingAckableRabbitMessage;

import io.opentracing.Span;
import io.opentracing.tag.Tags;

public class TracingAckableRabbitMessageFactory<T> extends ConfigurableRabbitMessageTracing<T> implements AckableRabbitMessageFactory<T> {

    public static final String REFERENCE_PARENT_SPAN_CONFIG = "tracing-ackable-reference-parent-span";

    @Override
    public AckableRabbitMessage<T> create(RabbitMessage<T> rabbitMessage, Acker acker, Consumer<? super Throwable> nacknowledger) {
        Span span = buildSpan(rabbitMessage).start();
        Acker decoratedAcker = decorateAcker(span, acker);
        Consumer<? super Throwable> decoratedNacknowledger = TracingDecoration.decorateNacknowledger(span, nacknowledger);
        return new TracingAckableRabbitMessage<>(tracer, span, rabbitMessage, decoratedAcker, decoratedNacknowledger);
    }

    @Override
    protected String getReferenceParentSpanConfig() {
        return REFERENCE_PARENT_SPAN_CONFIG;
    }

    @Override
    protected String getOperationName() {
        return "receive-ackable-rabbitmessage";
    }

    protected static Acker decorateAcker(Span span, Acker acker) {
        return ackType -> {
            span.log("acknowledged");
            span.setTag("ackType", ackType.name());
            span.setTag(Tags.ERROR.getKey(), false);
            span.finish();
            acker.ack(ackType);
        };
    }
}
