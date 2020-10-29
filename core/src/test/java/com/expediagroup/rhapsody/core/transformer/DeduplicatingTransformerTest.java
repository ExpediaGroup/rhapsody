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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;

import com.expediagroup.rhapsody.api.Deduplication;
import com.expediagroup.rhapsody.api.IdentityDeduplication;
import com.expediagroup.rhapsody.util.Defaults;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertEquals;

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

    @Test
    public void itemsAreNotDroppedUnderHeavyLoad() throws Exception {
        AtomicLong upstream = new AtomicLong(0L);
        AtomicLong downstream = new AtomicLong(0L);
        CountDownLatch latch = new CountDownLatch(1);

        Flux.fromStream(Stream.iterate(randomLong(CONFIG.getDeduplicationConcurrency()), last -> randomLong(CONFIG.getDeduplicationConcurrency())))
            .flatMap(number -> Flux.concat(
                Mono.just(number).repeat(CONFIG.getMaxDeduplicationSize() - 1),
                Mono.just(number).delayElement(randomDuration(CONFIG.getDeduplicationDuration().multipliedBy(2)))),
                CONFIG.getDeduplicationConcurrency())
            .take(Duration.ofSeconds(10L))
            .doOnNext(next -> upstream.incrementAndGet())
            .map(Collections::singletonList)
            .transform(DeduplicatingTransformer.identity(CONFIG, new Deduplication<List<Long>>() {

                @Override
                public Object extractKey(List<Long> longs) {
                    return longs.get(0);
                }

                @Override
                public List<Long> reduceDuplicates(List<Long> t1, List<Long> t2) {
                    return Stream.concat(t1.stream(), t2.stream()).collect(Collectors.toList());
                }
            }))
            .doOnNext(buffer -> {
                // Mimic real-world computationally-bound processing overhead
                long startNano = System.nanoTime();
                while (System.nanoTime() - startNano < 1_000_000) ;
            })
            .map(Collection::size)
            .subscribe(downstream::addAndGet, System.err::println, latch::countDown);

        latch.await();
        assertEquals(upstream.get(), downstream.get());
        System.out.println("Emitted: " + downstream.get());
    }

    private static long randomLong(long exclusiveUpperBound) {
        return (long) (Math.random() * exclusiveUpperBound);
    }

    private static Duration randomDuration(Duration exclusiveUpperBound) {
        return Duration.ofMillis((long) (Math.random() * exclusiveUpperBound.toMillis()));
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
