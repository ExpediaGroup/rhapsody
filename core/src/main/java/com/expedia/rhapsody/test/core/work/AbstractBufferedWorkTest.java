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
package com.expedia.rhapsody.test.core.work;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;

import com.expedia.rhapsody.api.PublisherFactory;
import com.expedia.rhapsody.api.SubscriberFactory;
import com.expedia.rhapsody.api.WorkType;
import com.expedia.rhapsody.core.adapter.Adapters;
import com.expedia.rhapsody.core.work.WorkBufferConfig;
import com.expedia.rhapsody.core.work.WorkBufferer;
import com.expedia.rhapsody.test.TestWork;
import com.expedia.rhapsody.util.Defaults;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public abstract class AbstractBufferedWorkTest {

    protected static final Duration STEP_DURATION = Duration.ofMillis(200);

    protected static final WorkBufferConfig WORK_BUFFER_CONFIG = new WorkBufferConfig(STEP_DURATION.multipliedBy(4), 8, Defaults.CONCURRENCY);

    protected static final String SUBJECT = "SUBJECT";

    protected final WorkBufferer<TestWork> workBufferer = WorkBufferer.identity(WORK_BUFFER_CONFIG);

    protected final SubscriberFactory<TestWork> subscriberFactory;

    protected final PublisherFactory<TestWork> publisherFactory;

    protected Consumer<TestWork> workConsumer;

    protected Flux<List<TestWork>> workFlux;

    protected AbstractBufferedWorkTest(SubscriberFactory<TestWork> subscriberFactory, PublisherFactory<TestWork> publisherFactory) {
        this.subscriberFactory = subscriberFactory;
        this.publisherFactory = publisherFactory;
    }

    @Before
    public void setup() {
        workConsumer = Adapters.toConsumer(subscriberFactory);
        workFlux = Flux.from(publisherFactory.create()).transform(workBufferer);
    }

    @Test
    public void bufferedCommitsAreImmediatelyEmitted() {
        TestWork commit = TestWork.create(WorkType.COMMIT, SUBJECT, Instant.now().toEpochMilli());

        StepVerifier.create(workFlux)
            .then(() -> workConsumer.accept(commit))
            .expectNext(Collections.singletonList(commit))
            .thenAwait(STEP_DURATION)
            .thenCancel()
            .verify();
    }

    @Test
    public void bufferedIntentsAndRetriesAreEmittedAfterBufferDuration() {
        TestWork intent = TestWork.create(WorkType.INTENT, SUBJECT, Instant.now().toEpochMilli());
        TestWork retry = TestWork.create(WorkType.RETRY, SUBJECT, intent.workHeader().inceptionEpochMilli() + 1);

        StepVerifier.create(workFlux)
            .then(() -> workConsumer.accept(intent))
            .expectNoEvent(STEP_DURATION)
            .then(() -> workConsumer.accept(retry))
            .expectNoEvent(STEP_DURATION)
            .expectNext(Arrays.asList(intent, retry))
            .thenAwait(WORK_BUFFER_CONFIG.getBufferDuration())
            .thenCancel()
            .verify();
    }

    @Test
    public void existingBuffersAreEmittedUponCommit() {
        TestWork intent = TestWork.create(WorkType.INTENT, SUBJECT, Instant.now().toEpochMilli());
        TestWork commit = TestWork.create(WorkType.COMMIT, SUBJECT, intent.workHeader().inceptionEpochMilli() + 1);

        StepVerifier.create(workFlux)
            .then(() -> workConsumer.accept(intent))
            .expectNoEvent(STEP_DURATION)
            .then(() -> workConsumer.accept(commit))
            .expectNext(Arrays.asList(intent, commit))
            .thenAwait(STEP_DURATION)
            .thenCancel()
            .verify();
    }
}
