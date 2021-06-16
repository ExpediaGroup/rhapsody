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
package com.expediagroup.rhapsody.core.work;

import java.util.List;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.api.Work;
import com.expediagroup.rhapsody.api.WorkHeader;
import com.expediagroup.rhapsody.api.WorkType;
import com.expediagroup.rhapsody.util.Defaults;

import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * This class conditionally "buffers" Work in to Lists that are bounded in Duration and size.
 * There is a known issue with `groupBy` and `flatMap` where the Stream can hang under heavy
 * load. Two approaches to mitigating this issue are increasing the buffer concurrency
 * and/or scaling buffering across more Publishers, i.e. more logical partitions.
 *
 * Reference:
 * https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html#groupBy-java.util.function.Function-
 */
public final class WorkBufferer<T> implements Function<Publisher<T>, Flux<List<T>>> {

    private static final Scheduler DEFAULT_SCHEDULER = Schedulers.newBoundedElastic(
        Defaults.THREAD_CAP, Integer.MAX_VALUE, WorkBufferer.class.getSimpleName());

    private final WorkBufferConfig config;

    private final Function<? super T, Work> workExtractor;

    private final Scheduler sourceScheduler;

    private WorkBufferer(WorkBufferConfig config, Function<? super T, Work> workExtractor, Scheduler sourceScheduler) {
        this.config = config;
        this.workExtractor = workExtractor;
        this.sourceScheduler = sourceScheduler;
    }

    public static <W extends Work> WorkBufferer<W> identity(WorkBufferConfig workBufferConfig) {
        return identity(workBufferConfig, DEFAULT_SCHEDULER);
    }

    public static <W extends Work> WorkBufferer<W> identity(WorkBufferConfig workBufferConfig, Scheduler sourceScheduler) {
        return new WorkBufferer<>(workBufferConfig, Function.identity(), sourceScheduler);
    }

    public static <W extends Work> WorkBufferer<Acknowledgeable<W>> acknowledgeable(WorkBufferConfig workBufferConfig) {
        return acknowledgeable(workBufferConfig, DEFAULT_SCHEDULER);
    }

    public static <W extends Work> WorkBufferer<Acknowledgeable<W>> acknowledgeable(WorkBufferConfig workBufferConfig, Scheduler sourceScheduler) {
        return new WorkBufferer<>(workBufferConfig, Acknowledgeable::get, sourceScheduler);
    }

    @Override
    public Flux<List<T>> apply(Publisher<T> publisher) {
        // - Use Scheduler with single worker for publishing, buffering, and subscribing
        //   (https://github.com/reactor/reactor-core/issues/2352)
        // - Each Subject gets its own Group
        // - Conditionally buffer max in-flight groups bounded in Duration and size
        Scheduler scheduler = Schedulers.single(sourceScheduler);
        return Flux.from(publisher)
            .publishOn(scheduler, config.getBufferSourcePrefetch())
            .groupBy(t -> extractHeader(t).subject())
            .flatMap(groupedFlux -> bufferGroup(groupedFlux, scheduler), config.getBufferConcurrency())
            .subscribeOn(scheduler);
    }

    private Mono<List<T>> bufferGroup(GroupedFlux<String, T> groupFlux, Scheduler scheduler) {
        return groupFlux.take(config.getBufferDuration(), scheduler)
            .take(config.getMaxBufferSize())
            .takeUntil(this::shouldCloseBufferForWork)
            .collectList();
    }

    private boolean shouldCloseBufferForWork(T t) {
        return extractHeader(t).type() == WorkType.COMMIT;
    }

    private WorkHeader extractHeader(T t) {
        return workExtractor.apply(t).workHeader();
    }
}
