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

import org.reactivestreams.Subscriber;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.api.PublisherFactory;
import com.expediagroup.rhapsody.api.SubscriberFactory;
import com.expediagroup.rhapsody.core.adapter.Adapters;

import reactor.core.publisher.Flux;
import reactor.core.publisher.WorkQueueProcessor;

public final class InMemoryFactory {

    private static final Map<String, WorkQueueProcessor> PROCESSORS_BY_NAME = new ConcurrentHashMap<>();

    private InMemoryFactory() {

    }

    public static <T> SubscriberFactory<T> subscriberFactory(String name) {
        return () -> subscriber(name);
    }

    public static <T> Subscriber<T> subscriber(String name) {
        return Adapters.toSubscriber(getNamedProcessor(name)::onNext);
    }

    public static <T> PublisherFactory<T> publisherFactory(String name) {
        return () -> publisher(name);
    }

    public static <T> Flux<Acknowledgeable<T>> acknowledegablePublisher(String name) {
        return InMemoryFactory.<T>publisher(name).map(Adapters::toLoggingAcknowledgeable);
    }

    public static <T> Flux<T> publisher(String name) {
        return getNamedProcessor(name);
    }

    private static <T> WorkQueueProcessor<T> getNamedProcessor(String name) {
        return (WorkQueueProcessor<T>) PROCESSORS_BY_NAME.computeIfAbsent(name, InMemoryFactory::createNamedProcessor);
    }

    private static WorkQueueProcessor createNamedProcessor(String name) {
        return WorkQueueProcessor.builder().name(name).share(true).build();
    }
}
