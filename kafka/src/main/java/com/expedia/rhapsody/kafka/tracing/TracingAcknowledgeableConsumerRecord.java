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
package com.expedia.rhapsody.kafka.tracing;

import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.expedia.rhapsody.api.AcknowledgeableFactory;
import com.expedia.rhapsody.api.ComposedAcknowledgeable;
import com.expedia.rhapsody.core.tracing.ComposedTracingAcknowledgeable;
import com.expedia.rhapsody.core.tracing.TracingAcknowledgeable;

import io.opentracing.Span;
import io.opentracing.Tracer;

public class TracingAcknowledgeableConsumerRecord<K, V> extends TracingAcknowledgeable<ConsumerRecord<K, V>> {

    private final Span span;

    private final ConsumerRecord<K, V> consumerRecord;

    private final Runnable acknowledger;

    private final Consumer<? super Throwable> nacknowledger;

    public TracingAcknowledgeableConsumerRecord(
        Tracer tracer,
        Span span,
        ConsumerRecord<K, V> consumerRecord,
        Runnable acknowledger,
        Consumer<? super Throwable> nacknowledger) {
        super(tracer);
        this.span = span;
        this.consumerRecord = consumerRecord;
        this.acknowledger = acknowledger;
        this.nacknowledger = nacknowledger;
    }

    @Override
    public ConsumerRecord<K, V> get() {
        return consumerRecord;
    }

    @Override
    public Runnable getAcknowledger() {
        return acknowledger;
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return nacknowledger;
    }

    @Override
    protected <R> AcknowledgeableFactory<R> createPropagator() {
        return (result, acknowledger, nacknowledger) ->
            new ComposedTracingAcknowledgeable<>(tracer, span, new ComposedAcknowledgeable<>(result, acknowledger, nacknowledger));
    }

    @Override
    protected Span span() {
        return span;
    }
}
