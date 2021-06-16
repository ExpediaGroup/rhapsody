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

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.Test;
import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.api.WorkType;
import com.expediagroup.rhapsody.test.TestWork;
import com.expediagroup.rhapsody.util.Defaults;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertEquals;

public class WorkBuffererTest {

    private static final Duration STEP_DURATION = Duration.ofMillis(200);

    private static final WorkBufferConfig BUFFER_CONFIG =
        new WorkBufferConfig(Defaults.PREFETCH, STEP_DURATION.multipliedBy(4), 8, Defaults.CONCURRENCY);

    private final Sinks.Many<TestWork> sink = Sinks.many().unicast().onBackpressureBuffer();

    private final Publisher<List<TestWork>> buffer = sink.asFlux().transform(WorkBufferer.identity(BUFFER_CONFIG));

    @Test
    public void bufferedCommitsAreImmediatelyEmitted() {
        TestWork commit1 = TestWork.create(WorkType.COMMIT, "SOME/URL/1");
        TestWork commit2 = TestWork.create(WorkType.COMMIT, "SOME/URL/2");

        StepVerifier.create(buffer)
            .then(() -> {
                sink.tryEmitNext(commit1);
                sink.tryEmitNext(commit2);
            })
            .expectNext(Collections.singletonList(commit1))
            .expectNext(Collections.singletonList(commit2))
            .thenCancel()
            .verify();
    }

    @Test
    public void bufferedNonCommitsAreEmittedAfterBufferDuration() {
        TestWork intent = TestWork.create(WorkType.INTENT, "SOME/URL");
        TestWork retry = TestWork.create(WorkType.RETRY, "SOME/URL", intent.workHeader().inceptionEpochMilli() + 1L);
        TestWork cancel = TestWork.create(WorkType.CANCEL, "SOME/URL", retry.workHeader().inceptionEpochMilli() + 1L);

        StepVerifier.create(buffer)
            .then(() -> {
                sink.tryEmitNext(intent);
                sink.tryEmitNext(retry);
                sink.tryEmitNext(cancel);
            })
            .expectNoEvent(BUFFER_CONFIG.getBufferDuration().minus(STEP_DURATION))
            .expectNext(Arrays.asList(intent, retry, cancel))
            .thenCancel()
            .verify();
    }

    @Test
    public void bufferedNonCommitsAreEmittedWithCommit() {
        TestWork intent = TestWork.create(WorkType.INTENT, "SOME/URL");
        TestWork retry = TestWork.create(WorkType.RETRY, "SOME/URL", intent.workHeader().inceptionEpochMilli() + 1L);
        TestWork cancel = TestWork.create(WorkType.CANCEL, "SOME/URL", retry.workHeader().inceptionEpochMilli() + 1L);
        TestWork commit = TestWork.create(WorkType.COMMIT, "SOME/URL", cancel.workHeader().inceptionEpochMilli() + 1L);

        StepVerifier.create(buffer)
            .then(() -> {
                sink.tryEmitNext(intent);
                sink.tryEmitNext(retry);
                sink.tryEmitNext(cancel);
            })
            .expectNoEvent(STEP_DURATION)
            .then(() -> sink.tryEmitNext(commit))
            .expectNext(Arrays.asList(intent, retry, cancel, commit))
            .thenCancel()
            .verify();
    }

    @Test
    public void interleavedWorkAreBufferedCorrectly() {
        TestWork intent1 = TestWork.create(WorkType.INTENT, "SOME/URL/1");
        TestWork commit1 = TestWork.create(WorkType.COMMIT, "SOME/URL/1", intent1.workHeader().inceptionEpochMilli() + 1L);

        TestWork intent2 = TestWork.create(WorkType.INTENT, "SOME/URL/2");
        TestWork commit2 = TestWork.create(WorkType.COMMIT, "SOME/URL/2", intent2.workHeader().inceptionEpochMilli() + 1L);

        StepVerifier.create(buffer)
            .then(() -> {
                sink.tryEmitNext(intent1);
                sink.tryEmitNext(intent2);
            })
            .expectNoEvent(STEP_DURATION)
            .then(() -> sink.tryEmitNext(commit2))
            .expectNext(Arrays.asList(intent2, commit2))
            .expectNoEvent(STEP_DURATION)
            .then(() -> sink.tryEmitNext(commit1))
            .expectNext(Arrays.asList(intent1, commit1))
            .thenCancel()
            .verify();
    }

    @Test
    public void workIsBufferedUpToMaxSize() {
        TestWork intent = TestWork.create(WorkType.INTENT, "SOME/URL");

        StepVerifier.create(buffer)
            .then(() -> LongStream.range(0, BUFFER_CONFIG.getMaxBufferSize() - 1).forEach(i -> sink.tryEmitNext(intent)))
            .expectNoEvent(STEP_DURATION)
            .then(() -> sink.tryEmitNext(intent))
            .expectNext(LongStream.range(0, BUFFER_CONFIG.getMaxBufferSize()).mapToObj(i -> intent).collect(Collectors.toList()))
            .thenCancel()
            .verify();
    }

    @Test
    public void workIsNotDroppedUnderHeavyLoad() throws Exception {
        AtomicLong upstream = new AtomicLong(0L);
        AtomicLong downstream = new AtomicLong(0L);
        CountDownLatch latch = new CountDownLatch(1);

        Flux.fromStream(Stream.iterate(randomLong(BUFFER_CONFIG.getBufferConcurrency()), last -> randomLong(BUFFER_CONFIG.getBufferConcurrency())))
            .map(number -> TestWork.create(WorkType.INTENT, Long.toString(number)))
            .flatMap(intent -> Flux.concat(
                Mono.just(intent),
                Mono.just(TestWork.create(WorkType.COMMIT, intent.workHeader().subject()))
                    .delayElement(randomDuration(BUFFER_CONFIG.getBufferDuration().multipliedBy(2)))),
                BUFFER_CONFIG.getBufferConcurrency())
            .take(Duration.ofSeconds(10L))
            .doOnNext(next -> upstream.incrementAndGet())
            .transform(WorkBufferer.identity(BUFFER_CONFIG))
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
}