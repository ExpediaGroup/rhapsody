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
package com.expediagroup.rhapsody.core.adapter;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.api.SubscriberFactory;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;

public final class Adapters {

    private Adapters() {

    }

    public static <T> Acknowledgeable<T> toLoggingAcknowledgeable(T t) {
        return new LoggingAcknowledgeable<>(t);
    }

    public static <T> Subscriber<Acknowledgeable<T>> toAcknowledgeableSubscriber(Subscriber<T> subscriber) {
        return toSendingSubscriber(flux -> flux.concatMap(Adapters::toTerminalAcknowledgement).subscribe(subscriber));
    }

    public static <T> Subscriber<T> toSendingSubscriber(Consumer<? super Flux<T>> sender) {
        return toSendingProcessor(sender);
    }

    public static <T> Subscriber<T> toSubscriber(Consumer<T> consumer) {
        return new ConsumingSubscriber<>(consumer);
    }

    /**
     * A SubscriberFactory is used to create a Consumer by wrapping each element as a singleton
     * Publisher to which NEW Subscribers produced by said Factory subscribe to. This is done
     * in order to respect Reactive Streams Rule 2.5 for Subscribers
     * (https://github.com/reactive-streams/reactive-streams-jvm#2.5).
     * If Subscriber creation or subscription is expensive, consider using `toSendingConsumer`.
     */
    public static <T> Consumer<T> toConsumer(SubscriberFactory<T> subscriberFactory) {
        return t -> Mono.just(t).subscribe(subscriberFactory.create());
    }

    /**
     * Creates a "sending" Consumer per producing Thread which helps to avoid both resource
     * contention and drain starvation when multiple Threads use the same Consumer. This should
     * provide a good performance trade-off between resource usage and thread safety when "sending"
     * is expensive. It is important to note that Consumers are NOT cleaned up, i.e. NOT removed
     * when no longer used. This means producing Threads MUST be constrained in lifecycle to that
     * of the application and that this should be called AT MOST ONCE per logical Sender in order
     * to avoid memory leakage.
     */
    public static <T> Consumer<T> toSendingConsumerPerThread(Consumer<? super Flux<T>> sender) {
        ThreadLocal<Consumer<T>> localConsumer = ThreadLocal.withInitial(() -> toSendingConsumer(sender));
        return t -> localConsumer.get().accept(t);
    }

    /**
     * A Sender is used to create a Consumer by creating a FluxProcessor (consumed by the Sender),
     * a FluxSink to which that FluxProcessor is subscribed, and a reference to sink::next.
     * Concurrent usage of the returned Consumer, while technically safe, should be avoided.
     * Otherwise, the underlying sink will be a source of resource contention. It will also put
     * calling Threads at risk of drain starvation, i.e. stuck draining backpressure buffer while
     * other Threads are producing to it.
     */
    public static <T> Consumer<T> toSendingConsumer(Consumer<? super Flux<T>> sender) {
        return toSendingProcessor(sender).sink()::next;
    }

    public static <T, R> Consumer<T> toSynchronousConsumer(Function<? super Flux<T>, ? extends Publisher<R>> sender) {
        return t -> Flux.just(t).publish(sender).ignoreElements().subscribeWith(new BlockableTerminationSubscriber<>()).block();
    }

    public static <T, R> Consumer<T> toSynchronousConsumer(Function<? super Flux<T>, ? extends Publisher<R>> sender, Duration timeout) {
        return t -> Flux.just(t).publish(sender).ignoreElements().subscribeWith(new BlockableTerminationSubscriber<>()).block(timeout);
    }

    /**
     * Note that we create a Processor with a buffer size of `1` in order to avoid adding a hidden
     * backpressure buffer. The Flux Consumer passed in can add backpressure buffers if it needs.
     */
    private static <T> FluxProcessor<T, T> toSendingProcessor(Consumer<? super Flux<T>> sender) {
        FluxProcessor<T, T> processor = EmitterProcessor.create(1);
        sender.accept(processor);
        return processor;
    }

    private static <T> Mono<T> toTerminalAcknowledgement(Acknowledgeable<T> acknowledgeable) {
        return Mono.just(acknowledgeable)
            .doAfterTerminate(acknowledgeable.getAcknowledger())
            .map(Acknowledgeable::get);
    }
}
