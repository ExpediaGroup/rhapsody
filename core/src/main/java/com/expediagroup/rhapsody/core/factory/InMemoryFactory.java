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
package com.expediagroup.rhapsody.core.factory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.reactivestreams.Subscriber;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.api.PublisherFactory;
import com.expediagroup.rhapsody.api.SubscriberFactory;
import com.expediagroup.rhapsody.core.adapter.Adapters;
import com.expediagroup.rhapsody.util.Defaults;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public final class InMemoryFactory {

    private static final Map<String, SinkFlux> SINK_FLUXES_BY_NAME = new ConcurrentHashMap<>();

    private InMemoryFactory() {

    }

    public static <T> SubscriberFactory<T> subscriberFactory(String name) {
        return () -> subscriber(name);
    }

    public static <T> Subscriber<T> subscriber(String name) {
        SinkFlux<T> sinkFlux = getNamedSinkFlux(name);
        return Adapters.toSubscriber(sinkFlux.sinkEmitter());
    }

    public static <T> PublisherFactory<T> publisherFactory(String name) {
        return () -> publisher(name);
    }

    public static <T> Flux<Acknowledgeable<T>> acknowledegablePublisher(String name) {
        return InMemoryFactory.<T>publisher(name).map(Adapters::toLoggingAcknowledgeable);
    }

    public static <T> Flux<T> publisher(String name) {
        return InMemoryFactory.<T>getNamedSinkFlux(name).flux();
    }

    private static <T> SinkFlux<T> getNamedSinkFlux(String name) {
        return (SinkFlux<T>) SINK_FLUXES_BY_NAME.computeIfAbsent(name, SinkFlux::named);
    }

    private static final class SinkFlux<T> {

        private final Sinks.Many<T> sink;

        private final Flux<T> flux;

        private SinkFlux(Sinks.Many<T> sink, Flux<T> flux) {
            this.sink = sink;
            this.flux = flux;
        }

        public static <T> SinkFlux<T> named(String name) {
            Sinks.Many<T> sink = Sinks.unsafe().many().multicast().onBackpressureBuffer(Integer.MAX_VALUE);
            Scheduler scheduler = Schedulers.newBoundedElastic(Defaults.THREAD_CAP, Integer.MAX_VALUE, name);
            return new SinkFlux<>(sink, sink.asFlux().publishOn(scheduler).publish().autoConnect());
        }

        public Consumer<T> sinkEmitter() {
            return t -> {
                synchronized (sink) {
                    sink.tryEmitNext(t);
                }
            };
        }

        public Flux<T> flux() {
            return flux;
        }
    }
}
