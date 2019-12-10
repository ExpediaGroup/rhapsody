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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.expediagroup.rhapsody.api.Acknowledgeable;

final class AcknowledgementQueuingSubscriber<T, A extends Acknowledgeable<T>> implements Subscriber<A>, Subscription {

    private static final AtomicLongFieldUpdater<AcknowledgementQueuingSubscriber> FREE_CAPACITY =
        AtomicLongFieldUpdater.newUpdater(AcknowledgementQueuingSubscriber.class, "freeCapacity");

    private static final AtomicLongFieldUpdater<AcknowledgementQueuingSubscriber> REQUEST_OUTSTANDING =
        AtomicLongFieldUpdater.newUpdater(AcknowledgementQueuingSubscriber.class, "requestOutstanding");

    private static final AtomicIntegerFieldUpdater<AcknowledgementQueuingSubscriber> REQUESTS_IN_PROGRESS =
        AtomicIntegerFieldUpdater.newUpdater(AcknowledgementQueuingSubscriber.class, "requestsInProgress");

    private final Map<Object, AcknowledgementQueue> queuesByGroup = new ConcurrentHashMap<>();

    private final Subscriber<? super Acknowledgeable<T>> actual;

    private final Function<T, ?> groupExtractor;

    private final Supplier<? extends AcknowledgementQueue> queueSupplier;

    private Subscription parent;

    private volatile long freeCapacity;

    private volatile long requestOutstanding;

    private volatile int requestsInProgress;

    AcknowledgementQueuingSubscriber(
        Subscriber<? super Acknowledgeable<T>> actual,
        Function<T, ?> groupExtractor,
        Supplier<? extends AcknowledgementQueue> queueSupplier,
        long maxInFlight) {
        this.actual = actual;
        this.queueSupplier = queueSupplier;
        this.groupExtractor = groupExtractor;
        this.freeCapacity = maxInFlight;
    }

    @Override
    public void onSubscribe(Subscription s) {
        parent = s;
        actual.onSubscribe(this);
    }

    @Override
    public void onNext(A a) {
        AcknowledgementQueue queue = queuesByGroup.computeIfAbsent(groupExtractor.apply(a.get()), group -> queueSupplier.get());

        AcknowledgementQueue.InFlight inFlight = queue.add(a.getAcknowledger(), a.getNacknowledger());

        actual.onNext(a.propagate(a.get(), () -> postComplete(queue.complete(inFlight)), error -> postComplete(queue.completeExceptionally(inFlight, error))));
    }

    @Override
    public void onError(Throwable t) {
        actual.onError(t);
    }

    @Override
    public void onComplete() {
        actual.onComplete();
    }

    @Override
    public void request(long requested) {
        if (requested > 0L) {
            REQUEST_OUTSTANDING.addAndGet(this, requested);
            drainRequest();
        }
    }

    @Override
    public void cancel() {
        parent.cancel();
    }

    private void postComplete(long drainedFromQueue) {
        if (freeCapacity != Long.MAX_VALUE && drainedFromQueue > 0L) {
            FREE_CAPACITY.addAndGet(this, drainedFromQueue);
            drainRequest();
        }
    }

    private void drainRequest() {
        if (REQUESTS_IN_PROGRESS.getAndIncrement(this) != 0) {
            return;
        }

        int missed = 1;
        do {
            long toRequest = Math.min(freeCapacity, requestOutstanding);

            if (toRequest > 0L) {
                if (freeCapacity != Long.MAX_VALUE) {
                    FREE_CAPACITY.addAndGet(this, -toRequest);
                }
                REQUEST_OUTSTANDING.addAndGet(this, -toRequest);
                parent.request(toRequest);
            }

            missed = REQUESTS_IN_PROGRESS.addAndGet(this, -missed);
        } while (missed != 0);
    }
}
