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
package com.expediagroup.rhapsody.core.transformer;

import java.time.Duration;

import org.junit.Test;
import org.reactivestreams.Subscription;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.UnicastProcessor;
import reactor.test.StepVerifier;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AutoAcknowledgingTransformerTest {

    private static final Duration STEP_DURATION = Duration.ofMillis(200);

    private static final AutoAcknowledgementConfig CONFIG = new AutoAcknowledgementConfig(STEP_DURATION.multipliedBy(2), STEP_DURATION.multipliedBy(4));

    private final FluxProcessor<AcknowledgeableData, AcknowledgeableData> processor = UnicastProcessor.create();

    private final FluxSink<AcknowledgeableData> sink = processor.sink();

    private final Flux<AcknowledgeableData> downstream = processor
        .transform(new AutoAcknowledgingTransformer<>(CONFIG, flux -> flux.takeLast(1), AcknowledgeableData::ack));

    @Test
    public void dataIsAcknowledgedOnlyAfterIntervalAndDelayElapses() {
        AcknowledgeableData data = new AcknowledgeableData();

        StepVerifier.create(downstream)
            .then(() -> sink.next(data))
            .expectNext(data)
            .thenAwait(CONFIG.getInterval())
            .then(() -> assertFalse(data.isAcked()))
            .thenAwait(CONFIG.getDelay().minus(STEP_DURATION))
            .then(() -> assertFalse(data.isAcked()))
            .thenAwait(STEP_DURATION.multipliedBy(2))
            .then(() -> assertTrue(data.isAcked()))
            .then(sink::complete)
            .expectComplete()
            .verify();
    }

    @Test
    public void dataIsSequentiallyAcknowledgedWithConcurrentWindows() {
        AcknowledgeableData data1 = new AcknowledgeableData();
        AcknowledgeableData data2 = new AcknowledgeableData();

        StepVerifier.create(downstream)
            .then(() -> sink.next(data1))
            .expectNext(data1)
            .thenAwait(CONFIG.getInterval().plus(STEP_DURATION))
            .then(() -> sink.next(data2))
            .expectNext(data2)
            .then(() -> assertFalse(data1.isAcked()))
            .then(() -> assertFalse(data2.isAcked()))
            .thenAwait(CONFIG.getDelay())
            .then(() -> assertTrue(data1.isAcked()))
            .then(() -> assertFalse(data2.isAcked()))
            .thenAwait(CONFIG.getInterval())
            .then(() -> assertTrue(data1.isAcked()))
            .then(() -> assertTrue(data2.isAcked()))
            .then(sink::complete)
            .expectComplete()
            .verify();
    }

    @Test
    public void dataIsAcknowledgedWhenUpstreamTerminates() {
        AcknowledgeableData data = new AcknowledgeableData();

        StepVerifier.create(downstream)
            .then(() -> sink.next(data))
            .expectNext(data)
            .then(sink::complete)
            .expectComplete()
            .verify();

        assertTrue(data.isAcked());
    }

    @Test
    public void dataIsAcknowledgedWhenDownstreamCancels() {
        AcknowledgeableData data = new AcknowledgeableData();

        StepVerifier.create(downstream)
            .then(() -> sink.next(data))
            .expectNext(data)
            .consumeSubscriptionWith(Subscription::cancel)
            .thenCancel()
            .verify();

        assertTrue(data.isAcked());
    }

    private static final class AcknowledgeableData {

        private boolean acked = false;

        public void ack() {
            acked = true;
        }

        public boolean isAcked() {
            return acked;
        }
    }
}