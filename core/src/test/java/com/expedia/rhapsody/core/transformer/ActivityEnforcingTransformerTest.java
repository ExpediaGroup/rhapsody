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
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import org.junit.Test;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;

public class ActivityEnforcingTransformerTest {

    private static final Duration STEP_DURATION = Duration.ofMillis(200);

    private static final ActivityEnforcementConfig CONFIG = new ActivityEnforcementConfig(STEP_DURATION.multipliedBy(10L), Duration.ZERO, STEP_DURATION.dividedBy(10L));

    private final FluxProcessor<String, String> processor = UnicastProcessor.create();

    private final Consumer<String> consumer = processor.sink()::next;

    private final Flux<String> downstream = processor.transform(new ActivityEnforcingTransformer<>(CONFIG));

    @Test
    public void errorIsEmittedIfStreamIsInactive() {
        StepVerifier.create(downstream)
            .expectSubscription()
            .expectNoEvent(CONFIG.getMaxInactivity().minus(STEP_DURATION))
            .expectError(TimeoutException.class)
            .verify();
    }

    @Test
    public void errorIsEmittedIfStreamBecomesInactiveAfterEvents() {
        StepVerifier.create(downstream)
            .thenAwait(STEP_DURATION.multipliedBy(2))
            .then(() -> consumer.accept("ONE"))
            .expectNextCount(1)
            .expectNoEvent(CONFIG.getMaxInactivity().minus(STEP_DURATION))
            .expectError(TimeoutException.class)
            .verify();
    }

    @Test
    public void errorIsNotEmittedIfStreamRemainsActive() {
        StepVerifier.create(downstream)
            .thenAwait(STEP_DURATION)
            .then(() -> consumer.accept("ONE"))
            .expectNextCount(1)
            .thenAwait(STEP_DURATION)
            .then(() -> consumer.accept("TWO"))
            .expectNextCount(1)
            .expectNoEvent(CONFIG.getMaxInactivity().minus(STEP_DURATION))
            .thenCancel()
            .verify();
    }
}