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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import com.expedia.rhapsody.api.AbstractAcknowledgeable;
import com.expedia.rhapsody.api.Acknowledgeable;
import com.expedia.rhapsody.api.Header;

import io.opentracing.Scope;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapInjectAdapter;

public abstract class TracingAcknowledgeable<T> extends AbstractAcknowledgeable<T> {

    protected final Tracer tracer;

    public TracingAcknowledgeable(Tracer tracer) {
        this.tracer = tracer;
    }

    @Override
    public Header header() {
        Map<String, String> headers = new HashMap<>();
        tracer.inject(span().context(), Format.Builtin.TEXT_MAP, new TextMapInjectAdapter(headers));
        return Header.fromMap(headers);
    }

    @Override
    public boolean filter(Predicate<? super T> predicate, Consumer<? super Acknowledgeable<T>> negativeConsumer) {
        return traceAround(() -> super.filter(predicate, negativeConsumer), "filter", predicate);
    }

    @Override
    public <R, C extends Collection<R>> Collection<Acknowledgeable<R>>
    mapToMany(Function<? super T, ? extends C> mapper, Consumer<? super Acknowledgeable<T>> emptyMappingConsumer) {
        return traceAround(() -> super.mapToMany(mapper, emptyMappingConsumer), "mapToMany", mapper);
    }

    @Override
    public <R> Acknowledgeable<R> map(Function<? super T, ? extends R> mapper) {
        return traceAround(() -> super.map(mapper), "map", mapper);
    }

    @Override
    public Acknowledgeable<T> reduce(BinaryOperator<T> reducer, Acknowledgeable<? extends T> other) {
        return traceAround(() -> super.reduce(reducer, other), "reduce", reducer);
    }

    @Override
    public void consume(Consumer<? super T> consumer, Consumer<? super Acknowledgeable<T>> andThen) {
        try (Scope scope = tracer.scopeManager().activate(span(), false)) {
            scope.span().log(formatEvent("start", "consume", consumer));
            consumer.accept(get());
            scope.span().log(formatEvent("finish", "consume", consumer));
        }
        // Run andThen Out-of-Scope since this instance strictly only traces operations on its
        // contained value, and it may (in fact, likely) be the case that andThen finishes the
        // Span around which the above Scope is wrapped. This guards against undefined behavior
        // when a Scope's underlying Span is finished before invocation of Scope::close.
        andThen.accept(this);
    }

    protected <R> R traceAround(Supplier<R> supplier, String operationType, Object operation) {
        try (Scope scope = tracer.scopeManager().activate(span(), false)) {
            scope.span().log(formatEvent("start", operationType, operation));
            R result = supplier.get();
            scope.span().log(formatEvent("finish", operationType, operation));
            return result;
        }
    }

    protected abstract Span span();

    protected static String formatEvent(String eventType, String operationType, Object operation) {
        return String.format("%s-%s-%s", eventType, operationType, operation.getClass().getSimpleName());
    }
}
