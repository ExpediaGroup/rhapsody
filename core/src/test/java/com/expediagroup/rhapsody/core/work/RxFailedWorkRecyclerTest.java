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
import java.time.Instant;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.Test;

import com.expediagroup.rhapsody.api.WorkType;
import com.expediagroup.rhapsody.test.TestWork;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class RxFailedWorkRecyclerTest {

    private static final WorkRecycleConfig RECYCLE_CONFIG =
        new WorkRecycleConfig(Duration.ofSeconds(10), 10L, Collections.singleton(IllegalStateException.class));

    private final AtomicReference<TestWork> recycleHolder = new AtomicReference<>();

    private final RxFailedWorkRecycler<TestWork, TestWork> recycler = new TestRxFailedWorkRecycler(RECYCLE_CONFIG, recycleHolder::set);

    @Test
    public void failedWorkIsNotRecycledIfExpired() {
        TestWork work = TestWork.create(WorkType.INTENT, "SOME/URL", Instant.now().minus(RECYCLE_CONFIG.getRecycleExpiration()).minusMillis(1).toEpochMilli());

        Flux.from(recycler.apply(work, new RuntimeException())).blockLast();

        assertNull(recycleHolder.get());
    }

    @Test
    public void failedWorkIsRecycledUntilMaxCountIsReached() {
        TestWork work = TestWork.create(WorkType.INTENT, "SOME/URL", Instant.now().toEpochMilli());

        for (long recycleCount = 0; recycleCount <= RECYCLE_CONFIG.getMaxRecycleCount(); recycleCount++) {
            Flux.from(recycler.apply(work, new RuntimeException())).blockLast();
            assertNotNull(recycleHolder.get());
            work = recycleHolder.get();
        }

        recycleHolder.set(null);

        Flux.from(recycler.apply(work, new RuntimeException())).blockLast();

        assertNull(recycleHolder.get());
    }

    @Test
    public void failedWorkIsNotRecycledForUnrecyclableException() {
        TestWork work = TestWork.create(WorkType.INTENT, "SOME/URL", Instant.now().toEpochMilli());

        Flux.from(recycler.apply(work, new IllegalStateException())).blockLast();
        assertNull(recycleHolder.get());

        Flux.from(recycler.apply(work, new RuntimeException())).blockLast();
        assertNotNull(recycleHolder.get());
    }

    private static final class TestRxFailedWorkRecycler extends RxFailedWorkRecycler<TestWork, TestWork> {

        public TestRxFailedWorkRecycler(WorkRecycleConfig config, Consumer<? super TestWork> workConsumer) {
            super(config, recycled -> Mono.just(recycled).doOnNext(workConsumer));
        }

        @Override
        protected TestWork recycle(TestWork work, Throwable error) {
            return TestWork.create(work.workHeader().recycle(WorkType.RETRY, error.getMessage()));
        }
    }
}