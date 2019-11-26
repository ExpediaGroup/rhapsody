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
package com.expedia.rhapsody.core.work;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.Test;

import com.expedia.rhapsody.api.AbstractAcknowledgeable;
import com.expedia.rhapsody.api.Acknowledgeable;
import com.expedia.rhapsody.api.AcknowledgeableFactory;
import com.expedia.rhapsody.api.ComposedAcknowledgeable;
import com.expedia.rhapsody.api.WorkReducer;
import com.expedia.rhapsody.api.WorkType;
import com.expedia.rhapsody.test.TestFailureReference;
import com.expedia.rhapsody.test.TestWork;
import com.expedia.rhapsody.test.TestWorkHeader;
import com.expedia.rhapsody.util.Translation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AcknowledgeableWorkBufferTranslatorTest {

    private final WorkReducer<TestWork> workReducer = new LatestWorkReducer<>();

    private final TestFailureReference<TestWork> failureReference = new TestFailureReference<>();

    @Test
    public void translatingEmptyBufferResultsInNothing() {
        AcknowledgeableWorkBufferTranslator<TestWork> translator =
            new AcknowledgeableWorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        Translation<List<Acknowledgeable<TestWork>>, Acknowledgeable<TestWork>> translation = translator.apply(Collections.emptyList());

        assertFalse(translation.hasResult());
        assertNull(failureReference.get());
    }

    @Test
    public void translatingUnpreparableWorkResultsInNoTranslationAndConsumedFailureOfLatest() {
        AcknowledgeableWorkBufferTranslator<TestWork> translator =
            new AcknowledgeableWorkBufferTranslator<>(workReducer, work -> { throw new IllegalArgumentException("Failed to prepare"); }, failureReference);

        Instant now = Instant.now();
        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli()));
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() + 1));
        AcknowledgeableTestWork acknowledgeableWork3 = new AcknowledgeableTestWork(TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() - 1));

        Translation<List<Acknowledgeable<TestWork>>, Acknowledgeable<TestWork>> translation =
            translator.apply(Arrays.asList(acknowledgeableWork3, acknowledgeableWork2, acknowledgeableWork1));

        assertFalse(translation.hasResult());
        assertEquals(acknowledgeableWork2.get(), failureReference.get());
        assertTrue(acknowledgeableWork1.isAcknowledged());
        assertTrue(acknowledgeableWork2.isAcknowledged());
        assertTrue(acknowledgeableWork3.isAcknowledged());
        assertFalse(acknowledgeableWork1.getError().isPresent());
        assertFalse(acknowledgeableWork2.getError().isPresent());
        assertFalse(acknowledgeableWork3.getError().isPresent());
    }

    @Test
    public void translatingPreparableWorkResultsInSuccessfulTranslationAndNoFailure() {
        AcknowledgeableWorkBufferTranslator<TestWork> translator =
            new AcknowledgeableWorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        TestWork work = TestWork.create(WorkType.COMMIT, "URL");
        AcknowledgeableTestWork acknowledgeableWork = new AcknowledgeableTestWork(work);

        Translation<List<Acknowledgeable<TestWork>>, Acknowledgeable<TestWork>> translation = translator.apply(Collections.singletonList(acknowledgeableWork));

        assertTrue(translation.hasResult());
        assertEquals(work, translation.getResult().get());
        assertNull(failureReference.get());
        assertFalse(acknowledgeableWork.isAcknowledged());
        assertFalse(acknowledgeableWork.getError().isPresent());
    }

    @Test
    public void translatingMultiplePreparableWorkResultsInSuccessfulTranslationOfLatest() {
        AcknowledgeableWorkBufferTranslator<TestWork> translator =
            new AcknowledgeableWorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        Instant now = Instant.now();
        TestWork work1 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() - 1L);
        TestWork work2 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli());
        TestWork work3 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() + 1L);

        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(work1);
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(work2);
        AcknowledgeableTestWork acknowledgeableWork3 = new AcknowledgeableTestWork(work3);

        List<Acknowledgeable<TestWork>> acknowledgeableWorks = Arrays.asList(acknowledgeableWork1, acknowledgeableWork2, acknowledgeableWork3);

        Translation<List<Acknowledgeable<TestWork>>, Acknowledgeable<TestWork>> translation = translator.apply(acknowledgeableWorks);

        assertTrue(translation.hasResult());
        assertEquals(work3, translation.getResult().get());
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
        AcknowledgeableWorkBufferTranslator<TestWork> translator =
            new AcknowledgeableWorkBufferTranslator<>(workReducer, work -> { throw new IllegalArgumentException("Failed to prepare"); }, failureReference);

        Instant now = Instant.now();
        TestWork work1 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() - 1L);
        TestWork work2 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli());
        TestWork work3 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() + 1L);

        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(work1);
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(work2);
        AcknowledgeableTestWork acknowledgeableWork3 = new AcknowledgeableTestWork(work3);

        List<Acknowledgeable<TestWork>> acknowledgeableWorks = Arrays.asList(acknowledgeableWork1, acknowledgeableWork2, acknowledgeableWork3);

        Translation<List<Acknowledgeable<TestWork>>, Acknowledgeable<TestWork>> translation = translator.apply(acknowledgeableWorks);

        assertFalse(translation.hasResult());
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
        AcknowledgeableWorkBufferTranslator<TestWork> translator =
            new AcknowledgeableWorkBufferTranslator<>(workReducer,
                work -> { throw new IllegalArgumentException("Failed to prepare"); },
                (testWork, throwable) -> { throw new IllegalArgumentException("Failed to fail"); });

        Instant now = Instant.now();
        TestWork work1 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() - 1L);
        TestWork work2 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli());
        TestWork work3 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() + 1L);

        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(work1);
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(work2);
        AcknowledgeableTestWork acknowledgeableWork3 = new AcknowledgeableTestWork(work3);

        List<Acknowledgeable<TestWork>> acknowledgeableWorks = Arrays.asList(acknowledgeableWork1, acknowledgeableWork2, acknowledgeableWork3);

        Translation<List<Acknowledgeable<TestWork>>, Acknowledgeable<TestWork>> translation = translator.apply(acknowledgeableWorks);

        assertFalse(translation.hasResult());
        assertFalse(acknowledgeableWork1.isAcknowledged());
        assertFalse(acknowledgeableWork2.isAcknowledged());
        assertFalse(acknowledgeableWork3.isAcknowledged());
        assertTrue(acknowledgeableWork1.getError().isPresent());
        assertTrue(acknowledgeableWork2.getError().isPresent());
        assertTrue(acknowledgeableWork3.getError().isPresent());
    }

    @Test
    public void canceledWorkIsNotTranslated() {
        AcknowledgeableWorkBufferTranslator<TestWork> translator =
            new AcknowledgeableWorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        AcknowledgeableTestWork acknowledgeableWork = new AcknowledgeableTestWork(TestWork.create(WorkType.CANCEL, "URL"));

        Translation<List<Acknowledgeable<TestWork>>, Acknowledgeable<TestWork>> translation =
            translator.apply(Collections.singletonList(acknowledgeableWork));

        assertFalse(translation.hasResult());
        assertNull(failureReference.get());
        assertFalse(acknowledgeableWork.getError().isPresent());
        assertEquals(1, acknowledgeableWork.getAcknowledgementCount());
    }

    @Test
    public void canceledWorkIsNotFailed() {
        AcknowledgeableWorkBufferTranslator<TestWork> translator =
            new AcknowledgeableWorkBufferTranslator<>(workReducer, work -> { throw new IllegalArgumentException("Failed to prepare"); }, failureReference);

        Instant now = Instant.now();
        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(TestWork.create(WorkType.INTENT, "URL", now.toEpochMilli() - 1L));
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(TestWork.create(WorkType.CANCEL, "URL", now.toEpochMilli()));

        Translation<List<Acknowledgeable<TestWork>>, Acknowledgeable<TestWork>> translation =
            translator.apply(Arrays.asList(acknowledgeableWork1, acknowledgeableWork2));

        assertFalse(translation.hasResult());
        assertEquals(acknowledgeableWork1.get(), failureReference.get());
        assertEquals(1, acknowledgeableWork1.getAcknowledgementCount());
        assertEquals(1, acknowledgeableWork2.getAcknowledgementCount());
    }

    @Test
    public void nonCommittedCanceledWorkWithSameMarkersAreNotTranslated() {
        String marker = UUID.randomUUID().toString();

        AcknowledgeableWorkBufferTranslator<TestWork> translator =
            new AcknowledgeableWorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(TestWork.create(TestWorkHeader.incept(WorkType.CANCEL, marker, "URL")));
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(TestWork.create(TestWorkHeader.incept(WorkType.INTENT, marker, "URL")));
        AcknowledgeableTestWork acknowledgeableWork3 = new AcknowledgeableTestWork(TestWork.create(TestWorkHeader.incept(WorkType.RETRY, marker, "URL")));

        Translation<List<Acknowledgeable<TestWork>>, Acknowledgeable<TestWork>> translation =
            translator.apply(Arrays.asList(acknowledgeableWork1, acknowledgeableWork2, acknowledgeableWork3));

        assertFalse(translation.hasResult());
        assertNull(failureReference.get());
        assertEquals(1, acknowledgeableWork1.getAcknowledgementCount());
        assertEquals(1, acknowledgeableWork2.getAcknowledgementCount());
        assertEquals(1, acknowledgeableWork2.getAcknowledgementCount());
    }

    @Test
    public void nonCanceledWorkWithDifferentMarkerIsTranslated() {
        AcknowledgeableWorkBufferTranslator<TestWork> translator =
            new AcknowledgeableWorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(TestWork.create(WorkType.CANCEL, "URL"));
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(TestWork.create(WorkType.INTENT, "URL"));

        Translation<List<Acknowledgeable<TestWork>>, Acknowledgeable<TestWork>> translation = translator.apply(Arrays.asList(acknowledgeableWork1, acknowledgeableWork2));

        assertTrue(translation.hasResult());
        assertEquals(acknowledgeableWork2.get(), translation.getResult().get());
        assertNull(failureReference.get());
        assertTrue(acknowledgeableWork1.isAcknowledged());
        assertFalse(acknowledgeableWork2.isAcknowledged());
        assertFalse(acknowledgeableWork2.getError().isPresent());
    }

    @Test
    public void committedButCanceledWorkIsTranslated() {
        AcknowledgeableWorkBufferTranslator<TestWork> translator =
            new AcknowledgeableWorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        String marker = UUID.randomUUID().toString();
        AcknowledgeableTestWork acknowledgeableWork1 = new AcknowledgeableTestWork(TestWork.create(TestWorkHeader.incept(WorkType.CANCEL, marker, "URL")));
        AcknowledgeableTestWork acknowledgeableWork2 = new AcknowledgeableTestWork(TestWork.create(TestWorkHeader.incept(WorkType.COMMIT, marker, "URL")));

        Translation<List<Acknowledgeable<TestWork>>, Acknowledgeable<TestWork>> translation =
            translator.apply(Arrays.asList(acknowledgeableWork1, acknowledgeableWork2));

        assertTrue(translation.hasResult());
        assertEquals(acknowledgeableWork2.get(), translation.getResult().get());
        assertNull(failureReference.get());
        assertTrue(acknowledgeableWork1.isAcknowledged());
        assertFalse(acknowledgeableWork2.isAcknowledged());
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