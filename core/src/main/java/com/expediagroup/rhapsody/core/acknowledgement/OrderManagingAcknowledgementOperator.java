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
package com.expediagroup.rhapsody.core.acknowledgement;

import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import com.expediagroup.rhapsody.api.Acknowledgeable;

public class OrderManagingAcknowledgementOperator<T, A extends Acknowledgeable<T>> implements Publisher<Acknowledgeable<T>> {

    private final Publisher<? extends A> source;

    private final Function<T, ?> groupExtractor;

    private final long maxInFlight;

    public OrderManagingAcknowledgementOperator(Publisher<? extends A> source, Function<T, ?> groupExtractor) {
        this(source, groupExtractor, Long.MAX_VALUE);
    }

    public OrderManagingAcknowledgementOperator(Publisher<? extends A> source, Function<T, ?> groupExtractor, long maxInFlight) {
        this.source = source;
        this.groupExtractor = groupExtractor;
        this.maxInFlight = maxInFlight;
    }

    @Override
    public void subscribe(Subscriber<? super Acknowledgeable<T>> actual) {
        source.subscribe(new AcknowledgementQueuingSubscriber<>(actual, groupExtractor, OrderManagingAcknowledgementQueue::newWithImmediateErrors, maxInFlight));
    }
}
