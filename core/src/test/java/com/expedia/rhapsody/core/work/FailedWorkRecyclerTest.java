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

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.Test;

import com.expedia.rhapsody.api.WorkType;
import com.expedia.rhapsody.test.TestWork;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class FailedWorkRecyclerTest {

    private static final WorkRecycleConfig RECYCLE_CONFIG = new WorkRecycleConfig(Duration.ofMillis(100), 10L, Collections.singleton(IllegalStateException.class));

    private final AtomicReference<TestWork> recycleHolder = new AtomicReference<>();

    private final FailedWorkRecycler<TestWork, TestWork> recycler = new TestFailedWorkRecycler(RECYCLE_CONFIG, recycleHolder::set);

    @Test
    public void failedWorkIsNotRecycledIfExpired() {
        TestWork work = TestWork.create(WorkType.INTENT, "SOME/URL", Instant.now().minus(RECYCLE_CONFIG.getRecycleExpiration()).minusMillis(1).toEpochMilli());

        recycler.accept(work, new RuntimeException());

        assertNull(recycleHolder.get());
    }

    @Test
    public void failedWorkIsRecycledUntilMaxCountIsReached() {
        TestWork work = TestWork.create(WorkType.INTENT, "SOME/URL", Instant.now().toEpochMilli());

        for (long recycleCount = 0; recycleCount <= RECYCLE_CONFIG.getMaxRecycleCount(); recycleCount++) {
            recycler.accept(work, new RuntimeException());
            assertNotNull(recycleHolder.get());
            work = recycleHolder.get();
        }

        recycleHolder.set(null);

        recycler.accept(work, new RuntimeException());
        assertNull(recycleHolder.get());
    }

    @Test
    public void failedWorkIsNotRecycledForUnrecyclableException() {
        TestWork work = TestWork.create(WorkType.INTENT, "SOME/URL", Instant.now().toEpochMilli());

        recycler.accept(work, new IllegalStateException());
        assertNull(recycleHolder.get());

        recycler.accept(work, new RuntimeException());
        assertNotNull(recycleHolder.get());
    }

    private static final class TestFailedWorkRecycler extends FailedWorkRecycler<TestWork, TestWork> {

        public TestFailedWorkRecycler(WorkRecycleConfig config, Consumer<? super TestWork> workConsumer) {
            super(config, workConsumer);
        }

        @Override
        protected TestWork recycle(TestWork work, Throwable error) {
            return TestWork.create(work.workHeader().recycle(WorkType.RETRY, error.getMessage()));
        }
    }
}