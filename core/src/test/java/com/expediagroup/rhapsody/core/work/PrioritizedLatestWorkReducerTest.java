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

import org.junit.Test;

import com.expediagroup.rhapsody.api.WorkReducer;
import com.expediagroup.rhapsody.api.WorkType;
import com.expediagroup.rhapsody.test.TestWork;

import static org.junit.Assert.assertEquals;

public class PrioritizedLatestWorkReducerTest {

    private final WorkReducer<TestWork> workReducer = new PrioritizedLatestWorkReducer<>();

    @Test
    public void reducingTwoWorksOfSameTypeResultsInLatest() {
        TestWork intent1 = createWork(WorkType.INTENT);
        TestWork intent2 = TestWork.create(WorkType.INTENT, "SUBJECT", intent1.workHeader().inceptionEpochMilli() + 1L);

        assertEquals(workReducer.reduceTry(intent1, intent2), workReducer.reduceFail(intent1, intent2));
        assertEquals(intent2, workReducer.reduceTry(intent1, intent2));
    }

    @Test
    public void reducingWorksOfDifferentTypeResultsInHighestPriority() {
        assertEquals(WorkType.COMMIT, workReducer.reduceTry(createWork(WorkType.COMMIT), createWork(WorkType.CANCEL)).workHeader().type());
        assertEquals(WorkType.COMMIT, workReducer.reduceTry(createWork(WorkType.COMMIT), createWork(WorkType.RETRY)).workHeader().type());
        assertEquals(WorkType.COMMIT, workReducer.reduceTry(createWork(WorkType.COMMIT), createWork(WorkType.INTENT)).workHeader().type());
        assertEquals(WorkType.CANCEL, workReducer.reduceTry(createWork(WorkType.RETRY), createWork(WorkType.CANCEL)).workHeader().type());
        assertEquals(WorkType.CANCEL, workReducer.reduceTry(createWork(WorkType.INTENT), createWork(WorkType.CANCEL)).workHeader().type());
        assertEquals(WorkType.RETRY, workReducer.reduceTry(createWork(WorkType.RETRY), createWork(WorkType.INTENT)).workHeader().type());
    }

    private TestWork createWork(WorkType workType) {
        return TestWork.create(workType, "SUBJECT");
    }
}