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
package com.expedia.rhapsody.core.tracing;

import java.util.function.Consumer;

import com.expedia.rhapsody.api.Acknowledgeable;
import com.expedia.rhapsody.api.AcknowledgeableFactory;
import com.expedia.rhapsody.api.ComposedAcknowledgeable;

import io.opentracing.Span;
import io.opentracing.Tracer;

public final class ComposedTracingAcknowledgeable<T> extends TracingAcknowledgeable<T> {

    private final Span span;

    private final Acknowledgeable<T> acknowledgeable;

    public ComposedTracingAcknowledgeable(Tracer tracer, Span span, Acknowledgeable<T> acknowledgeable) {
        super(tracer);
        this.span = span;
        this.acknowledgeable = acknowledgeable;
    }

    @Override
    public T get() {
        return acknowledgeable.get();
    }

    @Override
    public Runnable getAcknowledger() {
        return acknowledgeable.getAcknowledger();
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return acknowledgeable.getNacknowledger();
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
