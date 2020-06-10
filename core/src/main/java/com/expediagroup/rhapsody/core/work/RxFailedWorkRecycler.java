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

import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.rhapsody.api.RxFailureConsumer;
import com.expediagroup.rhapsody.api.Work;

import reactor.core.publisher.Mono;

public abstract class RxFailedWorkRecycler<W extends Work, R> implements RxFailureConsumer<W> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RxFailedWorkRecycler.class);

    private final WorkRecycleConfig config;

    private final Function<? super R, ? extends Publisher<?>> recyclePublisher;

    public RxFailedWorkRecycler(WorkRecycleConfig config, Function<? super R, ? extends Publisher<?>> recyclePublisher) {
        this.config = config;
        this.recyclePublisher = recyclePublisher;
    }

    @Override
    public Publisher<?> apply(W work, Throwable error) {
        if (isRecyclable(work, error)) {
            return Mono.just(work)
                .map(toRecycle -> recycle(toRecycle, error))
                .doOnNext(recycled -> hookOnRecycle(work, recycled, error))
                .flatMapMany(recyclePublisher);
        } else {
            return Mono.empty().doOnSubscribe(subscription -> hookOnDrop(work, error));
        }
    }

    protected boolean isRecyclable(W work, Throwable error) {
        return WorkRecycling.isRecyclable(config, work, error);
    }

    protected void hookOnRecycle(W work, R recycled, Throwable error) {
        LOGGER.warn("Recycling failure of Work={}: subject={} recycled={} error={}",
            work.getClass().getSimpleName(), work.workHeader().subject(), recycled, error.getMessage());
    }

    protected void hookOnDrop(W work, Throwable error) {
        LOGGER.warn("Dropping failed Work={}: subject={} error={}",
            work.getClass().getSimpleName(), work.workHeader().subject(), error.getMessage());
    }

    protected abstract R recycle(W work, Throwable error);
}
