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

import java.time.Duration;

import org.junit.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

public class RateLimitingTransformerTest {

    private static final Duration PERMIT_DURATION = Duration.ofMillis(500L);      // 2 Permits per Second

    private static final Duration STEP_DURATION = PERMIT_DURATION.dividedBy(5L);  // 5 "steps" per Permit

    private static final double PERMITS_PER_SECOND = Duration.ofMillis(1000L).dividedBy(PERMIT_DURATION.toMillis()).toMillis();

    private final RateLimitingConfig config = new RateLimitingConfig(PERMITS_PER_SECOND);

    @Test
    public void publishersCanBeRateLimited() {
        Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();

        Flux<String> rateLimitedFlux = sink.asFlux().publishOn(Schedulers.single())
            .transform(new RateLimitingTransformer<>(config));

        StepVerifier.create(rateLimitedFlux)
            .then(() -> {
                sink.tryEmitNext("ONE");
                sink.tryEmitNext("TWO");
                sink.tryEmitNext("THREE");
            })
            .expectNext("ONE")
            .expectNoEvent(PERMIT_DURATION.minus(STEP_DURATION))
            .expectNext("TWO")
            .expectNoEvent(PERMIT_DURATION.minus(STEP_DURATION))
            .expectNext("THREE")
            .thenCancel()
            .verify();
    }
}