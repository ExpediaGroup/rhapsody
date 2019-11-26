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
package com.expedia.rhapsody.test.core.acknowledgeable;

import java.time.Duration;
import java.util.function.Consumer;

import org.junit.Test;

import com.expedia.rhapsody.api.Acknowledgeable;
import com.expedia.rhapsody.api.PublisherFactory;
import com.expedia.rhapsody.api.SubscriberFactory;
import com.expedia.rhapsody.core.adapter.Adapters;

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

public abstract class AbstractAcknowledgeablePublishingTest {

    protected final Consumer<String> consumer;

    protected final PublisherFactory<Acknowledgeable<String>> acknowledgeablePublisherFactory;

    protected AbstractAcknowledgeablePublishingTest(SubscriberFactory<String> subscriberFactory, PublisherFactory<Acknowledgeable<String>> acknowledgeablePublisherFactory) {
        this.consumer = Adapters.toConsumer(subscriberFactory);
        this.acknowledgeablePublisherFactory = acknowledgeablePublisherFactory;
    }

    @Test
    public void acknowledgedDataIsNotRepublished() {
        consumer.accept("DATA");

        StepVerifier.create(acknowledgeablePublisherFactory.create())
            .consumeNextWith(Acknowledgeable::acknowledge)
            .thenCancel()
            .verify();

        StepVerifier.create(acknowledgeablePublisherFactory.create())
            .expectSubscription()
            .expectNoEvent(Duration.ofSeconds(10L))
            .thenCancel()
            .verify();
    }

    @Test
    public void unacknowledgedDataIsRepublished() {
        consumer.accept("DATA");

        StepVerifier.create(acknowledgeablePublisherFactory.create())
            .expectNextCount(1)
            .thenCancel()
            .verify();

        StepVerifier.create(acknowledgeablePublisherFactory.create())
            .expectNextCount(1)
            .thenCancel()
            .verify();
    }

    @Test
    public void nacknowledgedDataIsRepublished() {
        consumer.accept("DATA1");
        consumer.accept("DATA2");
        consumer.accept("DATA3");

        Flux.from(acknowledgeablePublisherFactory.create())
            .retry()
            .as(StepVerifier::create)
            .expectNextCount(1)
            .consumeNextWith(acknowledgeable -> acknowledgeable.nacknowledge(new RuntimeException()))
            .expectNextCount(2)
            .thenCancel()
            .verify();
    }
}
