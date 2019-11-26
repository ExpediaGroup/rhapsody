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
package com.expedia.rhapsody.core.transformer;

import java.util.function.Function;

import org.reactivestreams.Publisher;

import com.expedia.rhapsody.api.Acknowledgeable;
import com.expedia.rhapsody.api.Deduplication;

import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public final class DeduplicatingTransformer<T> implements Function<Publisher<T>, Publisher<T>> {

    private static final Scheduler DEDUPLICATING_SCHEDULER = Schedulers.newParallel(DeduplicatingTransformer.class.getSimpleName());

    private final DeduplicationConfig config;

    private final Deduplication<T> deduplication;

    private DeduplicatingTransformer(DeduplicationConfig config, Deduplication<T> deduplication) {
        this.config = config;
        this.deduplication = deduplication;
    }

    public static <T> DeduplicatingTransformer<T> identity(DeduplicationConfig config, Deduplication<T> deduplication) {
        return new DeduplicatingTransformer<>(config, deduplication);
    }

    public static <T> DeduplicatingTransformer<Acknowledgeable<T>> acknowledgeable(DeduplicationConfig config, Deduplication<T> deduplication) {
        return new DeduplicatingTransformer<>(config, new AcknowledgeableDeduplication<>(deduplication));
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        return config.isEnabled() ? applyDeduplication(publisher) : publisher;
    }

    private Flux<T> applyDeduplication(Publisher<T> publisher) {
        return Flux.from(publisher)
            .groupBy(deduplication::extractKey)
            .flatMap(this::deduplicateGroup, config.getDeduplicationConcurrency());
    }

    private Mono<T> deduplicateGroup(GroupedFlux<Object, T> groupedFlux) {
        return groupedFlux.take(config.getDeduplicationDuration(), DEDUPLICATING_SCHEDULER)
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
