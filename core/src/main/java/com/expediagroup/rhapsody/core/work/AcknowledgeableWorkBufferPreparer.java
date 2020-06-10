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
import com.expediagroup.rhapsody.api.RxFailureConsumer;
import com.expediagroup.rhapsody.api.RxWorkPreparer;
import com.expediagroup.rhapsody.api.Work;
import com.expediagroup.rhapsody.api.WorkReducer;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class AcknowledgeableWorkBufferPreparer<W extends Work> implements Function<List<Acknowledgeable<W>>, Publisher<Acknowledgeable<W>>> {

    private final WorkReducer<W> workReducer;

    private final RxWorkPreparer<W> workPreparer;

    private final RxFailureConsumer<W> failureConsumer;

    public AcknowledgeableWorkBufferPreparer(WorkReducer<W> workReducer, RxWorkPreparer<W> workPreparer, RxFailureConsumer<W> failureConsumer) {
        this.workReducer = workReducer;
        this.workPreparer = workPreparer;
        this.failureConsumer = failureConsumer;
    }

    @Override
    public Publisher<Acknowledgeable<W>> apply(List<Acknowledgeable<W>> buffer) {
        List<Acknowledgeable<W>> nonCanceledBuffer = WorkBuffers.collectNonCanceledAcknowledgeable(buffer, Acknowledgeable::acknowledge);
        return Flux.fromIterable(nonCanceledBuffer)
            .reduce(Acknowledgeable.reducing(workReducer::reduceTry))
            .flatMap(this::prepareIfNecessary)
            .onErrorResume(error -> handleNonCanceledPreparationError(nonCanceledBuffer, error));
    }

    private Mono<Acknowledgeable<W>> prepareIfNecessary(Acknowledgeable<W> work) {
        return work.get().isPrepared() ? Mono.just(work) :
            Mono.from(workPreparer.prepare(work.get())).map(prepared -> work.propagate(prepared, work.getAcknowledger(), work.getNacknowledger()));
    }

    private Mono<? extends Acknowledgeable<W>> handleNonCanceledPreparationError(List<Acknowledgeable<W>> nonCanceledBuffer, Throwable error) {
        return Flux.fromIterable(nonCanceledBuffer)
            .reduce(Acknowledgeable.reducing(workReducer::reduceFail))
            .flatMapMany(toFail -> consumeFailure(toFail, error))
            .then(Mono.empty());
    }

    private Flux<?> consumeFailure(Acknowledgeable<W> toFail, Throwable error) {
        return Flux.from(failureConsumer.apply(toFail.get(), error))
            .doOnCancel(toFail.getAcknowledger())
            .doOnComplete(toFail.getAcknowledger())
            .doOnError(toFail.getNacknowledger())
            .onErrorResume(fatalError -> Mono.empty());
    }
}
