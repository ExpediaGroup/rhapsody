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

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.junit.Test;
import org.reactivestreams.Subscriber;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.api.SubscriberFactory;
import com.expediagroup.rhapsody.test.TestAcknowledgeable;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AdaptersTest {

    @Test
    public void adaptedAcknowledgeableSubscribersAcknowledgeOnDownstreamError() {
        TestAcknowledgeable acknowledgeable = new TestAcknowledgeable("DATA");

        Subscriber<String> downstream = Adapters.toSubscriber(string -> {
            throw new IllegalArgumentException();
        });

        Sinks.Many<Acknowledgeable<String>> sink = Sinks.many().multicast().onBackpressureBuffer();
        sink.asFlux().subscribe(Adapters.toAcknowledgeableSubscriber(downstream));
        sink.tryEmitNext(acknowledgeable);

        assertTrue(acknowledgeable.isAcknowledged());
    }

    @Test
    public void consumerDoesNotDeadlockOnFailureWithRetry() {
        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();
        SubscriberFactory<String> subscriberFactory = () -> Adapters.toSubscriber(sink::tryEmitNext);

        handleWithErrorAndRetry(sink.asFlux()).subscribe();

        Adapters.toConsumer(subscriberFactory).accept("Hello");
    }

    @Test
    public void sendingCanBeMadeSynchronous() {
        AtomicBoolean terminated = new AtomicBoolean(false);
        Consumer<String> consumer = Adapters.toSynchronousConsumer(flux ->
            flux.subscribeOn(Schedulers.parallel()).doOnComplete(() -> terminated.set(true)));

        consumer.accept("Hello");
        assertTrue(terminated.get());
    }

    @Test
    public void synchronousRuntimeExceptionsArePropagatedFromConsumer() {
        Consumer<String> consumer = Adapters.toSynchronousConsumer(flux ->
            flux.subscribeOn(Schedulers.parallel()).handle((string, sink) -> sink.error(new IllegalArgumentException())));

        try {
            consumer.accept("Hello");
            fail("Consumer should have thrown an Exception");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void synchronousCheckedExceptionsArePropagatedFromConsumer() {
        Consumer<String> consumer = Adapters.toSynchronousConsumer(flux ->
            flux.subscribeOn(Schedulers.parallel()).handle((string, sink) -> sink.error(new IOException())));

        try {
            consumer.accept("Hello");
            fail("Consumer should have thrown an Exception");
        } catch (Exception e) {
            assertTrue(e instanceof RuntimeException);
            assertTrue(e.getCause() instanceof IOException);
        }
    }

    @Test
    public void synchronousConsumptionTimeoutsThrowException() {
        Consumer<String> consumer = Adapters.toSynchronousConsumer(flux ->
            Flux.never().subscribeOn(Schedulers.parallel()), Duration.ofMillis(100L));

        try {
            consumer.accept("Hello");
            fail("Consumer should have thrown an Exception");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalStateException);
        }
    }

    @Test
    public void synchronousConsumerDoesNotDeadlockOnFailureWithRetry() {
        Adapters.toSynchronousConsumer(this::handleWithErrorAndRetry).accept("Hello");
    }

    private <T> Flux<T> handleWithErrorAndRetry(Flux<T> flux) {
        return flux.<T>handle((string, sink) -> sink.error(new RuntimeException())).retry();
    }
}