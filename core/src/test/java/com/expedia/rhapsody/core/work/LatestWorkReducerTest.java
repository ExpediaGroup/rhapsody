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

import java.util.Optional;
import java.util.stream.Stream;

import org.junit.Test;

import com.expedia.rhapsody.api.WorkReducer;
import com.expedia.rhapsody.api.WorkType;
import com.expedia.rhapsody.test.TestWork;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LatestWorkReducerTest {

    private final WorkReducer<TestWork> workReducer = new LatestWorkReducer<>();

    @Test
    public void reducingTryFromMultipleResultsInLatest() {
        TestWork work1 = TestWork.create(WorkType.INTENT, "URL");
        TestWork work2 = TestWork.create(WorkType.RETRY, "URL", work1.workHeader().inceptionEpochMilli() - 1L);
        TestWork work3 = TestWork.create(WorkType.COMMIT, "URL", work1.workHeader().inceptionEpochMilli() + 1L);

        Optional<TestWork> tried = Stream.of(work1, work3, work2).reduce(workReducer::reduceTry);
        Optional<TestWork> failed = Stream.of(work1, work3, work2).reduce(workReducer::reduceFail);

        assertTrue(tried.isPresent());
        assertEquals(work3, tried.get());
        assertEquals(tried, failed);
    }
}