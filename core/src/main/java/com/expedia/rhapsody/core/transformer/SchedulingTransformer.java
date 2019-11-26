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

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

public final class SchedulingTransformer<T> implements Function<Publisher<T>, Flux<T>> {

    private final Scheduler scheduler;

    private final SchedulingConfig config;

    public SchedulingTransformer(SchedulingConfig config, String name) {
        this.scheduler = config.createScheduler(name);
        this.config = config;
    }

    @Override
    public Flux<T> apply(Publisher<T> publisher) {
        return Flux.from(publisher).publishOn(scheduler, config.isDelayError(), config.getPrefetch());
    }
}
