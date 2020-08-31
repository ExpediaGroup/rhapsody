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
import java.util.function.Consumer;

import org.junit.Test;

import com.expediagroup.rhapsody.api.Deduplication;
import com.expediagroup.rhapsody.api.IdentityDeduplication;
import com.expediagroup.rhapsody.util.Defaults;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;

public class DeduplicatingTransformerTest {

    private static final DeduplicationConfig CONFIG =
        new DeduplicationConfig(Defaults.PREFETCH, Duration.ofMillis(800), 4, Defaults.CONCURRENCY);

    private final FluxProcessor<String, String> processor = UnicastProcessor.create();

    private final Consumer<String> consumer = processor.sink()::next;

    private final Flux<String> downstream = processor.transform(DeduplicatingTransformer.identity(CONFIG, new IdentityDeduplication<>()));

    @Test
    public void duplicatesAreNotEmitted() {
        StepVerifier.withVirtualTime(() -> downstream)
            .expectSubscription()
            .then(() -> {
                consumer.accept("ONE");
                consumer.accept("ONE");
            })
            .expectNoEvent(CONFIG.getDeduplicationDuration())
            .expectNext("ONE")
            .expectNoEvent(CONFIG.getDeduplicationDuration())
            .thenCancel()
            .verify();
    }

    @Test
    public void deduplicatesOnlyWithinDuration() {
        StepVerifier.withVirtualTime(() -> downstream)
            .expectSubscription()
            .then(() -> consumer.accept("ONE"))
            .thenAwait(CONFIG.getDeduplicationDuration())
            .expectNext("ONE")
            .then(() -> consumer.accept("ONE"))
            .thenAwait(CONFIG.getDeduplicationDuration())
            .expectNext("ONE")
            .expectNoEvent(CONFIG.getDeduplicationDuration())
            .thenCancel()
            .verify();
    }

    @Test
    public void deduplicatesOnlyWithinMaxSize() {
        StepVerifier.withVirtualTime(() -> downstream)
            .expectSubscription()
            .then(() -> {
                consumer.accept("ONE");
                consumer.accept("ONE");
                consumer.accept("ONE");
                consumer.accept("ONE");
                consumer.accept("ONE");
            })
            .expectNext("ONE")
            .expectNoEvent(CONFIG.getDeduplicationDuration())
            .expectNext("ONE")
            .expectNoEvent(CONFIG.getDeduplicationDuration())
            .thenCancel()
            .verify();

    }

    @Test
    public void dataAreSequentiallyProcessed() {
        Flux<String> invertedDownstream = processor.transform(DeduplicatingTransformer.identity(CONFIG, new InvertedReducerDeduplication()));

        StepVerifier.create(invertedDownstream)
            .expectSubscription()
            .then(() -> consumer.accept("ONE"))
            .thenAwait(Duration.ofMillis(200))
            .then(() -> {
                consumer.accept("TWO");
                consumer.accept("TWO");
                consumer.accept("ONE");
            })
            .thenAwait(CONFIG.getDeduplicationDuration())
            .expectNext("ONE")
            .expectNext("TWO")
            .expectNoEvent(CONFIG.getDeduplicationDuration())
            .thenCancel()
            .verify();
    }

    private static final class InvertedReducerDeduplication implements Deduplication<String> {

        public String extractKey(String data) {
            return data;
        }

        public String reduceDuplicates(String s1, String s2) {
            return s2;
        }
    }
}
