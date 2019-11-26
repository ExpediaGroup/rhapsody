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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.reactivestreams.Publisher;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import reactor.core.publisher.Flux;

public final class MetricsTransformer<T> implements Function<Publisher<T>, Publisher<T>> {

    private final AtomicInteger subscribers = new AtomicInteger();

    private final Counter items;

    private final Counter errors;

    public MetricsTransformer(MetricsConfig config, MeterRegistry meterRegistry) {
        Gauge.builder(config.getName() + ".subscribers", subscribers, Number::doubleValue)
            .tags(config.getTags())
            .baseUnit("subscribers")
            .description("Number of current Subscribers")
            .register(meterRegistry);

        this.items = Counter.builder(config.getName() + ".items")
            .tags(config.getTags())
            .baseUnit("items")
            .description("Number of Items emitted")
            .register(meterRegistry);

        this.errors = Counter.builder(config.getName() + ".errors")
            .tags(config.getTags())
            .baseUnit("errors")
            .description("Number of Errors emitted")
            .register(meterRegistry);
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        return Flux.from(publisher)
            .doOnSubscribe(subscription -> subscribers.incrementAndGet())
            .doOnNext(t -> items.increment())
            .doOnError(error -> errors.increment())
            .doFinally(signalType -> subscribers.decrementAndGet());
    }
}
