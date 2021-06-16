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
package com.expediagroup.rhapsody.core.transformer;

import java.util.function.Function;

import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.api.Deduplication;
import com.expediagroup.rhapsody.util.Defaults;

import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public final class DeduplicatingTransformer<T> implements Function<Publisher<T>, Publisher<T>> {

    private static final Scheduler DEFAULT_SCHEDULER = Schedulers.newBoundedElastic(
        Defaults.THREAD_CAP, Integer.MAX_VALUE, DeduplicatingTransformer.class.getSimpleName());

    private final DeduplicationConfig config;

    private final Deduplication<T> deduplication;

    private final Scheduler sourceScheduler;

    private DeduplicatingTransformer(DeduplicationConfig config, Deduplication<T> deduplication, Scheduler sourceScheduler) {
        this.config = config;
        this.deduplication = deduplication;
        this.sourceScheduler = sourceScheduler;
    }

    public static <T> DeduplicatingTransformer<T> identity(DeduplicationConfig config, Deduplication<T> deduplication) {
        return identity(config, deduplication, DEFAULT_SCHEDULER);
    }

    public static <T> DeduplicatingTransformer<T> identity(DeduplicationConfig config, Deduplication<T> deduplication, Scheduler sourceScheduler) {
        return new DeduplicatingTransformer<>(config, deduplication, sourceScheduler);
    }

    public static <T> DeduplicatingTransformer<Acknowledgeable<T>>
    acknowledgeable(DeduplicationConfig config, Deduplication<T> deduplication) {
        return acknowledgeable(config, deduplication, DEFAULT_SCHEDULER);
    }

    public static <T> DeduplicatingTransformer<Acknowledgeable<T>>
    acknowledgeable(DeduplicationConfig config, Deduplication<T> deduplication, Scheduler sourceScheduler) {
        return new DeduplicatingTransformer<>(config, new AcknowledgeableDeduplication<>(deduplication), sourceScheduler);
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        return config.isEnabled() ? applyDeduplication(publisher) : publisher;
    }

    private Flux<T> applyDeduplication(Publisher<T> publisher) {
        // - Use Scheduler with single worker for publishing, buffering, and subscribing
        //   (https://github.com/reactor/reactor-core/issues/2352)
        // - Each deduplication key gets its own Group
        // - Buffer max in-flight groups bounded in Duration and size
        Scheduler scheduler = Schedulers.single(sourceScheduler);
        return Flux.from(publisher)
            .publishOn(scheduler, config.getDeduplicationSourcePrefetch())
            .groupBy(deduplication::extractKey)
            .flatMap(groupedFlux -> deduplicateGroup(groupedFlux, scheduler), config.getDeduplicationConcurrency())
            .subscribeOn(scheduler);
    }

    private Mono<T> deduplicateGroup(GroupedFlux<Object, T> groupedFlux, Scheduler scheduler) {
        return groupedFlux.take(config.getDeduplicationDuration(), scheduler)
            .take(config.getMaxDeduplicationSize())
            .reduce(deduplication::reduceDuplicates);
    }

    private static final class AcknowledgeableDeduplication<T> implements Deduplication<Acknowledgeable<T>> {

        private final Deduplication<T> deduplication;

        public AcknowledgeableDeduplication(Deduplication<T> deduplication) {
            this.deduplication = deduplication;
        }

        @Override
        public Object extractKey(Acknowledgeable<T> acknowledgeable) {
            return deduplication.extractKey(acknowledgeable.get());
        }

        @Override
        public Acknowledgeable<T> reduceDuplicates(Acknowledgeable<T> t1, Acknowledgeable<T> t2) {
            return t1.reduce(deduplication::reduceDuplicates, t2);
        }
    }
}
