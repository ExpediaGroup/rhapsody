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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.Test;

import com.expediagroup.rhapsody.api.AbstractAcknowledgeable;
import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.api.AcknowledgeableFactory;
import com.expediagroup.rhapsody.api.ComposedAcknowledgeable;
import com.expediagroup.rhapsody.api.WorkType;
import com.expediagroup.rhapsody.test.TestFailureReference;
import com.expediagroup.rhapsody.test.TestWork;
import com.expediagroup.rhapsody.test.TestWorkHeader;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AcknowledgeableWorkBufferPreparerTest {

    private final TestFailureReference<TestWork> failureReference = new TestFailureReference<>();

    @Test
    public void preparingEmptyBufferResultsInNothing() {
        AcknowledgeableWorkBufferPreparer<TestWork> preparer = new AcknowledgeableWorkBufferPreparer<>(
            new LatestWorkReducer<>(),
            work -> Mono.just(work.prepare()),
            (work, error) -> Mono.empty().doOnSubscribe(subscription -> failureReference.accept(work, error)));

        List<Acknowledgeable<TestWork>> prepared = Flux.from(preparer.apply(Collections.emptyList())).collectList().block();

        assertNotNull(prepared);
        assertTrue(prepared.isEmpty());
        assertNull(failureReference.get());
    }

    @Test
    public void preparingUnpreparableWorkResultsInNoEmissionAndPublishedFailureOfLatest() {
        AcknowledgeableWorkBufferPreparer<TestWork> preparer = new AcknowledgeableWorkBufferPreparer<>(
            new LatestWorkReducer<>(),
            work -> Mono.error(new IllegalArgumentException("Failed to prepare")),
            (work, error) -> Mono.empty().doOnSubscribe(subscription -> failureReference.accept(work, error)));

        Instant now = Instant.now();
        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli()));
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() + 1));
        AcknowledgeableTestWork acknowledgeableWork3 = new AcknowledgeableTestWork(TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() - 1));

        List<Acknowledgeable<TestWork>> buffer = Arrays.asList(acknowledgeableWork3, acknowledgeableWork2, acknowledgeableWork1);
        List<Acknowledgeable<TestWork>> prepared = Flux.from(preparer.apply(buffer)).collectList().block();

        assertNotNull(prepared);
        assertTrue(prepared.isEmpty());
        assertEquals(acknowledgeableWork2.get(), failureReference.get());
        assertTrue(acknowledgeableWork1.isAcknowledged());
        assertTrue(acknowledgeableWork2.isAcknowledged());
        assertTrue(acknowledgeableWork3.isAcknowledged());
        assertFalse(acknowledgeableWork1.getError().isPresent());
        assertFalse(acknowledgeableWork2.getError().isPresent());
        assertFalse(acknowledgeableWork3.getError().isPresent());
    }

    @Test
    public void preparingWorkResultsInSuccessfulEmissionAndNoFailure() {
        AcknowledgeableWorkBufferPreparer<TestWork> preparer = new AcknowledgeableWorkBufferPreparer<>(
            new LatestWorkReducer<>(),
            work -> Mono.just(work.prepare()),
            (work, error) -> Mono.empty().doOnSubscribe(subscription -> failureReference.accept(work, error)));

        TestWork work = TestWork.create(WorkType.COMMIT, "URL");
        AcknowledgeableTestWork acknowledgeableWork = new AcknowledgeableTestWork(work);

        List<Acknowledgeable<TestWork>> prepared = Flux.from(preparer.apply(Collections.singletonList(acknowledgeableWork))).collectList().block();

        assertNotNull(prepared);
        assertEquals(1, prepared.size());
        assertEquals(work, prepared.get(0).get());
        assertNull(failureReference.get());
        assertFalse(acknowledgeableWork.isAcknowledged());
        assertFalse(acknowledgeableWork.getError().isPresent());
    }

    @Test
    public void preparingMultipleWorkResultsInSuccessfulEmissionOfLatest() {
        AcknowledgeableWorkBufferPreparer<TestWork> preparer = new AcknowledgeableWorkBufferPreparer<>(
            new LatestWorkReducer<>(),
            work -> Mono.just(work.prepare()),
            (work, error) -> Mono.empty().doOnSubscribe(subscription -> failureReference.accept(work, error)));

        Instant now = Instant.now();
        TestWork work1 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() - 1L);
        TestWork work2 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli());
        TestWork work3 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() + 1L);

        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(work1);
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(work2);
        AcknowledgeableTestWork acknowledgeableWork3 = new AcknowledgeableTestWork(work3);

        List<Acknowledgeable<TestWork>> buffer = Arrays.asList(acknowledgeableWork3, acknowledgeableWork2, acknowledgeableWork1);
        List<Acknowledgeable<TestWork>> prepared = Flux.from(preparer.apply(buffer)).collectList().block();

        assertNotNull(prepared);
        assertEquals(1, prepared.size());
        assertEquals(work3, prepared.get(0).get());
        assertNull(failureReference.get());
        assertFalse(acknowledgeableWork1.isAcknowledged());
        assertFalse(acknowledgeableWork2.isAcknowledged());
        assertFalse(acknowledgeableWork3.isAcknowledged());
        assertFalse(acknowledgeableWork1.getError().isPresent());
        assertFalse(acknowledgeableWork2.getError().isPresent());
        assertFalse(acknowledgeableWork3.getError().isPresent());
    }

    @Test
    public void successfullyHandlingFailureOfPreparationAcknowledges() {
        AcknowledgeableWorkBufferPreparer<TestWork> preparer = new AcknowledgeableWorkBufferPreparer<>(
            new LatestWorkReducer<>(),
            work -> Mono.error(new IllegalArgumentException("Failed to prepare")),
            (work, error) -> Mono.empty().doOnSubscribe(subscription -> failureReference.accept(work, error)));

        Instant now = Instant.now();
        TestWork work1 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() - 1L);
        TestWork work2 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli());
        TestWork work3 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() + 1L);

        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(work1);
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(work2);
        AcknowledgeableTestWork acknowledgeableWork3 = new AcknowledgeableTestWork(work3);

        List<Acknowledgeable<TestWork>> buffer = Arrays.asList(acknowledgeableWork3, acknowledgeableWork2, acknowledgeableWork1);
        List<Acknowledgeable<TestWork>> prepared = Flux.from(preparer.apply(buffer)).collectList().block();

        assertNotNull(prepared);
        assertTrue(prepared.isEmpty());
        assertEquals(work3, failureReference.get());
        assertTrue(acknowledgeableWork1.isAcknowledged());
        assertTrue(acknowledgeableWork2.isAcknowledged());
        assertTrue(acknowledgeableWork3.isAcknowledged());
        assertFalse(acknowledgeableWork1.getError().isPresent());
        assertFalse(acknowledgeableWork2.getError().isPresent());
        assertFalse(acknowledgeableWork3.getError().isPresent());
    }

    @Test
    public void failingToHandleFailureNacknowledges() {
        AcknowledgeableWorkBufferPreparer<TestWork> preparer = new AcknowledgeableWorkBufferPreparer<>(
            new LatestWorkReducer<>(),
            work -> Mono.error(new IllegalArgumentException("Failed to prepare")),
            (work, error) -> Mono.error(new IllegalArgumentException("Failed to fail")));

        Instant now = Instant.now();
        TestWork work1 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() - 1L);
        TestWork work2 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli());
        TestWork work3 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() + 1L);

        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(work1);
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(work2);
        AcknowledgeableTestWork acknowledgeableWork3 = new AcknowledgeableTestWork(work3);

        List<Acknowledgeable<TestWork>> buffer = Arrays.asList(acknowledgeableWork3, acknowledgeableWork2, acknowledgeableWork1);
        List<Acknowledgeable<TestWork>> prepared = Flux.from(preparer.apply(buffer)).collectList().block();

        assertNotNull(prepared);
        assertTrue(prepared.isEmpty());
        assertFalse(acknowledgeableWork1.isAcknowledged());
        assertFalse(acknowledgeableWork2.isAcknowledged());
        assertFalse(acknowledgeableWork3.isAcknowledged());
        assertTrue(acknowledgeableWork1.getError().isPresent());
        assertTrue(acknowledgeableWork2.getError().isPresent());
        assertTrue(acknowledgeableWork3.getError().isPresent());
    }

    @Test
    public void preparationFailureDoesNotAcknowledgeOrNacknowledgeUntilFailureIsHandled() throws Exception {
        CompletableFuture<Void> proceedWithFailure = new CompletableFuture<>();
        CompletableFuture<Void> failureSubscribed = new CompletableFuture<>();
        AcknowledgeableWorkBufferPreparer<TestWork> preparer = new AcknowledgeableWorkBufferPreparer<>(
            new LatestWorkReducer<>(),
            work -> Mono.error(new IllegalArgumentException("Failed to prepare")),
            (work, error) -> Mono.fromFuture(proceedWithFailure).doOnSubscribe(subscription -> failureSubscribed.complete(null)));

        AcknowledgeableTestWork acknowledgeableWork = new AcknowledgeableTestWork(TestWork.create(WorkType.INTENT, "URL"));

        Future<Void> emitted = Flux.from(preparer.apply(Collections.singletonList(acknowledgeableWork)))
            .subscribeOn(Schedulers.elastic())
            .then()
            .toFuture();

        failureSubscribed.get(10, TimeUnit.SECONDS);

        assertFalse(acknowledgeableWork.isAcknowledged());
        assertFalse(acknowledgeableWork.getError().isPresent());
        assertFalse(emitted.isDone());

        proceedWithFailure.complete(null);
        emitted.get(10, TimeUnit.SECONDS);

        assertTrue(acknowledgeableWork.isAcknowledged());
        assertFalse(acknowledgeableWork.getError().isPresent());
        assertTrue(emitted.isDone());
    }

    @Test
    public void canceledWorkIsNotPrepared() {
        AcknowledgeableWorkBufferPreparer<TestWork> preparer = new AcknowledgeableWorkBufferPreparer<>(
            new LatestWorkReducer<>(),
            work -> Mono.just(work.prepare()),
            (work, error) -> Mono.empty().doOnSubscribe(subscription -> failureReference.accept(work, error)));

        AcknowledgeableTestWork acknowledgeableWork = new AcknowledgeableTestWork(TestWork.create(WorkType.CANCEL, "URL"));

        List<Acknowledgeable<TestWork>> prepared = Flux.from(preparer.apply(Collections.singletonList(acknowledgeableWork))).collectList().block();

        assertNotNull(prepared);
        assertTrue(prepared.isEmpty());
        assertNull(failureReference.get());
        assertFalse(acknowledgeableWork.getError().isPresent());
        assertEquals(1, acknowledgeableWork.getAcknowledgementCount());
    }

    @Test
    public void canceledWorkIsNotFailed() {
        AcknowledgeableWorkBufferPreparer<TestWork> preparer = new AcknowledgeableWorkBufferPreparer<>(
            new LatestWorkReducer<>(),
            work -> Mono.error(new IllegalArgumentException("Failed to prepare")),
            (work, error) -> Mono.empty().doOnSubscribe(subscription -> failureReference.accept(work, error)));

        Instant now = Instant.now();
        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(TestWork.create(WorkType.INTENT, "URL", now.toEpochMilli() - 1L));
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(TestWork.create(WorkType.CANCEL, "URL", now.toEpochMilli()));

        List<Acknowledgeable<TestWork>> buffer = Arrays.asList(acknowledgeableWork1, acknowledgeableWork2);
        List<Acknowledgeable<TestWork>> prepared = Flux.from(preparer.apply(buffer)).collectList().block();

        assertNotNull(prepared);
        assertTrue(prepared.isEmpty());
        assertEquals(acknowledgeableWork1.get(), failureReference.get());
        assertEquals(1, acknowledgeableWork1.getAcknowledgementCount());
        assertEquals(1, acknowledgeableWork2.getAcknowledgementCount());
    }

    @Test
    public void nonCommittedCanceledWorkWithSameMarkersAreNotEmitted() {
        AcknowledgeableWorkBufferPreparer<TestWork> preparer = new AcknowledgeableWorkBufferPreparer<>(
            new LatestWorkReducer<>(),
            work -> Mono.just(work.prepare()),
            (work, error) -> Mono.empty().doOnSubscribe(subscription -> failureReference.accept(work, error)));

        String marker = UUID.randomUUID().toString();
        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(TestWork.create(TestWorkHeader.incept(WorkType.CANCEL, marker, "URL")));
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(TestWork.create(TestWorkHeader.incept(WorkType.INTENT, marker, "URL")));
        AcknowledgeableTestWork acknowledgeableWork3 = new AcknowledgeableTestWork(TestWork.create(TestWorkHeader.incept(WorkType.RETRY, marker, "URL")));

        List<Acknowledgeable<TestWork>> buffer = Arrays.asList(acknowledgeableWork3, acknowledgeableWork2, acknowledgeableWork1);
        List<Acknowledgeable<TestWork>> prepared = Flux.from(preparer.apply(buffer)).collectList().block();

        assertNotNull(prepared);
        assertTrue(prepared.isEmpty());
        assertNull(failureReference.get());
        assertEquals(1, acknowledgeableWork1.getAcknowledgementCount());
        assertEquals(1, acknowledgeableWork2.getAcknowledgementCount());
        assertEquals(1, acknowledgeableWork3.getAcknowledgementCount());
    }

    @Test
    public void nonCanceledWorkWithDifferentMarkerIsEmitted() {
        AcknowledgeableWorkBufferPreparer<TestWork> preparer = new AcknowledgeableWorkBufferPreparer<>(
            new LatestWorkReducer<>(),
            work -> Mono.just(work.prepare()),
            (work, error) -> Mono.empty().doOnSubscribe(subscription -> failureReference.accept(work, error)));

        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(TestWork.create(WorkType.CANCEL, "URL"));
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(TestWork.create(WorkType.INTENT, "URL"));

        List<Acknowledgeable<TestWork>> buffer = Arrays.asList(acknowledgeableWork2, acknowledgeableWork1);
        List<Acknowledgeable<TestWork>> prepared = Flux.from(preparer.apply(buffer)).collectList().block();

        assertNotNull(prepared);
        assertEquals(1, prepared.size());
        assertEquals(acknowledgeableWork2.get(), prepared.get(0).get());
        assertNull(failureReference.get());
        assertTrue(acknowledgeableWork1.isAcknowledged());
        assertFalse(acknowledgeableWork2.isAcknowledged());
        assertFalse(acknowledgeableWork2.getError().isPresent());
    }

    @Test
    public void committedButCanceledWorkIsEmitted() {
        AcknowledgeableWorkBufferPreparer<TestWork> preparer = new AcknowledgeableWorkBufferPreparer<>(
            new LatestWorkReducer<>(),
            work -> Mono.just(work.prepare()),
            (work, error) -> Mono.empty().doOnSubscribe(subscription -> failureReference.accept(work, error)));

        String marker = UUID.randomUUID().toString();
        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(TestWork.create(TestWorkHeader.incept(WorkType.CANCEL, marker, "URL")));
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(TestWork.create(TestWorkHeader.incept(WorkType.COMMIT, marker, "URL")));

        List<Acknowledgeable<TestWork>> buffer = Arrays.asList(acknowledgeableWork2, acknowledgeableWork1);
        List<Acknowledgeable<TestWork>> prepared = Flux.from(preparer.apply(buffer)).collectList().block();

        assertNotNull(prepared);
        assertEquals(1, prepared.size());
        assertEquals(acknowledgeableWork2.get(), prepared.get(0).get());
        assertNull(failureReference.get());
        assertTrue(acknowledgeableWork1.isAcknowledged());
        assertFalse(acknowledgeableWork2.isAcknowledged());
        assertFalse(acknowledgeableWork2.getError().isPresent());
    }

    private static final class AcknowledgeableTestWork extends AbstractAcknowledgeable<TestWork> {

        private final TestWork testWork;

        private final AtomicInteger acknowledgedCount = new AtomicInteger();

        private final AtomicReference<Throwable> nacknowledged = new AtomicReference<>();

        private AcknowledgeableTestWork(TestWork testWork) {
            this.testWork = testWork;
        }

        @Override
        public TestWork get() {
            return testWork;
        }

        @Override
        public Runnable getAcknowledger() {
            return acknowledgedCount::incrementAndGet;
        }

        @Override
        public Consumer<? super Throwable> getNacknowledger() {
            return nacknowledged::set;
        }

        public boolean isAcknowledged() {
            return acknowledgedCount.get() > 0;
        }

        public int getAcknowledgementCount() {
            return acknowledgedCount.get();
        }

        public Optional<Throwable> getError() {
            return Optional.ofNullable(nacknowledged.get());
        }

        @Override
        protected <R> AcknowledgeableFactory<R> createPropagator() {
            return ComposedAcknowledgeable::new;
        }
    }
}