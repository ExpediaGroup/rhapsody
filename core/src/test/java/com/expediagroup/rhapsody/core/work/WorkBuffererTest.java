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
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.junit.Test;
import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.api.WorkType;
import com.expediagroup.rhapsody.test.TestWork;
import com.expediagroup.rhapsody.util.Defaults;

import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;

public class WorkBuffererTest {

    private static final Duration STEP_DURATION = Duration.ofMillis(200);

    private static final WorkBufferConfig BUFFER_CONFIG = new WorkBufferConfig(STEP_DURATION.multipliedBy(4), 8, Defaults.CONCURRENCY);

    private final FluxProcessor<TestWork, TestWork> eventQueue = UnicastProcessor.create();

    private final Consumer<TestWork> eventConsumer = eventQueue.sink()::next;

    private final Publisher<List<TestWork>> buffer = eventQueue.transform(WorkBufferer.identity(BUFFER_CONFIG));

    @Test
    public void bufferedCommitsAreImmediatelyEmitted() {
        TestWork commit1 = TestWork.create(WorkType.COMMIT, "SOME/URL/1");
        TestWork commit2 = TestWork.create(WorkType.COMMIT, "SOME/URL/2");

        StepVerifier.create(buffer)
            .then(() -> {
                eventConsumer.accept(commit1);
                eventConsumer.accept(commit2);
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
                eventConsumer.accept(intent);
                eventConsumer.accept(retry);
                eventConsumer.accept(cancel);
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
                eventConsumer.accept(intent);
                eventConsumer.accept(retry);
                eventConsumer.accept(cancel);
            })
            .expectNoEvent(STEP_DURATION)
            .then(() -> eventConsumer.accept(commit))
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
                eventConsumer.accept(intent1);
                eventConsumer.accept(intent2);
            })
            .expectNoEvent(STEP_DURATION)
            .then(() -> eventConsumer.accept(commit2))
            .expectNext(Arrays.asList(intent2, commit2))
            .expectNoEvent(STEP_DURATION)
            .then(() -> eventConsumer.accept(commit1))
            .expectNext(Arrays.asList(intent1, commit1))
            .thenCancel()
            .verify();
    }

    @Test
    public void workIsBufferedUpToMaxSize() {
        TestWork intent = TestWork.create(WorkType.INTENT, "SOME/URL");

        StepVerifier.create(buffer)
            .then(() -> LongStream.range(0, BUFFER_CONFIG.getMaxBufferSize() - 1).forEach(i -> eventConsumer.accept(intent)))
            .expectNoEvent(STEP_DURATION)
            .then(() -> eventConsumer.accept(intent))
            .expectNext(LongStream.range(0, BUFFER_CONFIG.getMaxBufferSize()).mapToObj(i -> intent).collect(Collectors.toList()))
            .thenCancel()
            .verify();
    }
}