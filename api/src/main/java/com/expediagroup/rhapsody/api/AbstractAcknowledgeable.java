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
package com.expediagroup.rhapsody.api;

import java.util.Collection;
import java.util.Collections;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import org.reactivestreams.Publisher;

/**
 * Base functionality for Acknowledgeable implementations. Extensions need only implement
 * {@link Acknowledgeable#get() get},
 * {@link Acknowledgeable#getAcknowledger() getAcknowledger},
 * {@link Acknowledgeable#getNacknowledger() getNacknowledger}, and
 * {@link Acknowledgeable#propagate propagate}
 *
 * @param <T> The type of data item
 */
public abstract class AbstractAcknowledgeable<T> implements Acknowledgeable<T> {

    @Override
    public Header header() {
        return Headed.tryCast(get())
            .map(Headed::header)
            .orElseGet(Header::empty);
    }

    @Override
    public boolean filter(Predicate<? super T> predicate, Consumer<? super Acknowledgeable<T>> negativeConsumer) {
        boolean result = predicate.test(get());
        if (!result) {
            negativeConsumer.accept(this);
        }
        return result;
    }

    @Override
    public <R, C extends Collection<R>> Collection<Acknowledgeable<R>>
    mapToMany(Function<? super T, ? extends C> mapper, Consumer<? super Acknowledgeable<T>> emptyMappingConsumer) {
        C collection = mapper.apply(get());
        if (collection.isEmpty()) {
            emptyMappingConsumer.accept(this);
        }
        return collection.isEmpty() ? Collections.emptyList() : new AcknowledgingCollection<>(collection, getAcknowledger(), getNacknowledger(), createPropagator());
    }

    @Override
    public <R> Acknowledgeable<R> map(Function<? super T, ? extends R> mapper) {
        return propagate(mapper.apply(get()), getAcknowledger(), getNacknowledger());
    }

    @Override
    public <R, P extends Publisher<R>> Publisher<Acknowledgeable<R>> publish(Function<? super T, ? extends P> mapper) {
        return new AcknowledgingPublisher<>(mapper.apply(get()), getAcknowledger(), getNacknowledger(), createPropagator());
    }

    @Override
    public Acknowledgeable<T> reduce(BinaryOperator<T> reducer, Acknowledgeable<? extends T> other) {
        return propagate(reducer.apply(get(), other.get()),
            Acknowledgeable.combineAcknowledgers(getAcknowledger(), other.getAcknowledger()),
            Acknowledgeable.combineNacknowledgers(getNacknowledger(), other.getNacknowledger()));
    }

    @Override
    public void consume(Consumer<? super T> consumer, Consumer<? super Acknowledgeable<T>> andThen) {
        consumer.accept(get());
        andThen.accept(this);
    }

    @Override
    public final <R> Acknowledgeable<R> propagate(R result, Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        return this.<R>createPropagator().create(result, acknowledger, nacknowledger);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "(" + get() + ")";
    }

    protected abstract <R> AcknowledgeableFactory<R> createPropagator();
}
