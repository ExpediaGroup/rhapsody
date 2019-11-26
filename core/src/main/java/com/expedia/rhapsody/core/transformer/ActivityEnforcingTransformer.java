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

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class ActivityEnforcingTransformer<T> implements Function<Publisher<T>, Publisher<T>> {

    private final ActivityEnforcementConfig config;

    public ActivityEnforcingTransformer(ActivityEnforcementConfig config) {
        this.config = config;
    }

    @Override
    public Publisher<T> apply(Publisher<T> publisher) {
        return config.isEnabled() ? enforceActivity(publisher) : publisher;
    }

    private Flux<T> enforceActivity(Publisher<T> publisher) {
        AtomicReference<Instant> lastActive = new AtomicReference<>(Instant.now());
        return Flux.merge(createInactivityError(lastActive::get), publisher)
            .doOnEach(signal -> lastActive.set(Instant.now()));
    }

    private Mono<T> createInactivityError(Supplier<Instant> lastActive) {
        return Flux.interval(config.getDelay(), config.getInterval())
            .map(i -> lastActive.get())
            .filter(this::hasBecomeInactiveSince)
            .next()
            .flatMap(this::createInactivityError);
    }

    private boolean hasBecomeInactiveSince(Instant lastActive) {
        return config.getMaxInactivity().compareTo(Duration.between(lastActive, Instant.now())) < 0;
    }

    private Mono<T> createInactivityError(Instant lastActive) {
        return Mono.error(new InactiveStreamException(config.getName(), config.getMaxInactivity(), lastActive));
    }

    private static final class InactiveStreamException extends TimeoutException {

        public InactiveStreamException(String name, Duration maxInactivity, Instant lastActive) {
            super(String.format("Stream=%s has been inactive for longer than duration=%s with lastActive=%s", name, maxInactivity, lastActive));
        }
    }
}
