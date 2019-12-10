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
import java.util.UUID;

import org.junit.Test;

import com.expediagroup.rhapsody.api.WorkReducer;
import com.expediagroup.rhapsody.api.WorkType;
import com.expediagroup.rhapsody.test.TestFailureReference;
import com.expediagroup.rhapsody.test.TestWork;
import com.expediagroup.rhapsody.test.TestWorkHeader;
import com.expediagroup.rhapsody.util.Translation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class WorkBufferTranslatorTest {

    private final WorkReducer<TestWork> workReducer = new LatestWorkReducer<>();

    private final TestFailureReference<TestWork> failureReference = new TestFailureReference<>();

    @Test
    public void translatingEmptyBufferResultsInNothing() {
        WorkBufferTranslator<TestWork> translator =
            new WorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        Translation<List<TestWork>, TestWork> translation = translator.apply(Collections.emptyList());

        assertFalse(translation.hasResult());
        assertNull(failureReference.get());
    }

    @Test
    public void translatingUnpreparableWorkResultsInNoTranslationAndConsumedFailureOfLatest() {
        WorkBufferTranslator<TestWork> translator =
            new WorkBufferTranslator<>(workReducer, work -> { throw new IllegalArgumentException("Failed to prepare"); }, failureReference);

        Instant now = Instant.now();
        TestWork work1 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli());
        TestWork work2 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() + 1);
        TestWork work3 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() - 1);

        Translation<List<TestWork>, TestWork> translation = translator.apply(Arrays.asList(work3, work2, work1));

        assertFalse(translation.hasResult());
        assertEquals(work2, failureReference.get());
    }

    @Test
    public void translatingPreparableWorkResultsInSuccessfulTranslationAndNoFailure() {
        WorkBufferTranslator<TestWork> translator =
            new WorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        TestWork work = TestWork.create(WorkType.COMMIT, "URL");

        Translation<List<TestWork>, TestWork> translation = translator.apply(Collections.singletonList(work));

        assertTrue(translation.hasResult());
        assertEquals(work, translation.getResult());
        assertNull(failureReference.get());
    }

    @Test
    public void translatingMultiplePreparableWorkResultsInSuccessfulTranslationOfLatest() {
        WorkBufferTranslator<TestWork> translator =
            new WorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        Instant now = Instant.now();
        TestWork work1 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() - 1L);
        TestWork work2 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli());
        TestWork work3 = TestWork.create(WorkType.COMMIT, "URL", now.toEpochMilli() + 1L);

        Translation<List<TestWork>, TestWork> translation = translator.apply(Arrays.asList(work1, work3, work2));

        assertTrue(translation.hasResult());
        assertEquals(work3, translation.getResult());
        assertNull(failureReference.get());
    }

    @Test
    public void canceledWorkIsNotTranslated() {
        WorkBufferTranslator<TestWork> translator =
            new WorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        TestWork work = TestWork.create(WorkType.CANCEL, "URL");

        Translation<List<TestWork>, TestWork> translation = translator.apply(Collections.singletonList(work));

        assertFalse(translation.hasResult());
        assertNull(failureReference.get());
    }

    @Test
    public void canceledWorkIsNotFailed() {
        WorkBufferTranslator<TestWork> translator =
            new WorkBufferTranslator<>(workReducer, work -> { throw new IllegalArgumentException("Failed to prepare"); }, failureReference);

        Instant now = Instant.now();
        TestWork work1 = TestWork.create(WorkType.INTENT, "URL", now.toEpochMilli() - 1L);
        TestWork work2 = TestWork.create(WorkType.CANCEL, "URL", now.toEpochMilli());

        Translation<List<TestWork>, TestWork> translation = translator.apply(Arrays.asList(work1, work2));

        assertFalse(translation.hasResult());
        assertEquals(work1, failureReference.get());
    }

    @Test
    public void nonCommittedCanceledWorkWithSameMarkersAreNotTranslated() {
        String marker = UUID.randomUUID().toString();

        WorkBufferTranslator<TestWork> translator =
            new WorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        TestWork work1 = TestWork.create(TestWorkHeader.incept(WorkType.CANCEL, marker, "URL"));
        TestWork work2 = TestWork.create(TestWorkHeader.incept(WorkType.INTENT, marker, "URL"));
        TestWork work3 = TestWork.create(TestWorkHeader.incept(WorkType.RETRY, marker, "URL"));

        Translation<List<TestWork>, TestWork> translation = translator.apply(Arrays.asList(work1, work2, work3));

        assertFalse(translation.hasResult());
        assertNull(failureReference.get());
    }

    @Test
    public void nonCanceledWorkWithDifferentMarkerIsTranslated() {
        WorkBufferTranslator<TestWork> translator =
            new WorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        TestWork work1 = TestWork.create(WorkType.CANCEL, "URL");
        TestWork work2 = TestWork.create(WorkType.INTENT, "URL");

        Translation<List<TestWork>, TestWork> translation = translator.apply(Arrays.asList(work1, work2));

        assertTrue(translation.hasResult());
        assertEquals(work2, translation.getResult());
        assertNull(failureReference.get());
    }

    @Test
    public void committedButCanceledWorkIsTranslated() {
        WorkBufferTranslator<TestWork> translator =
            new WorkBufferTranslator<>(workReducer, TestWork::prepare, failureReference);

        String marker = UUID.randomUUID().toString();
        TestWork work1 = TestWork.create(TestWorkHeader.incept(WorkType.CANCEL, marker, "URL"));
        TestWork work2 = TestWork.create(TestWorkHeader.incept(WorkType.COMMIT, marker, "URL"));

        Translation<List<TestWork>, TestWork> translation = translator.apply(Arrays.asList(work1, work2));

        assertTrue(translation.hasResult());
        assertEquals(work2, translation.getResult());
        assertNull(failureReference.get());
    }
}