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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.test.TestAcknowledgeable;
import com.expediagroup.rhapsody.util.Timing;

import reactor.core.publisher.EmitterProcessor;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.FluxSink;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AcknowledgementQueuingSubscriberTest {

    protected static final Executor EXECUTOR = Executors.newCachedThreadPool();

    @Test
    public void acknowledgementOnlyEverExecutesOnOneThreadInNonBlockingFashion() throws Exception {
        CompletableFuture<Boolean> firstAcknowledgementStarted = new CompletableFuture<>();
        CompletableFuture<Boolean> firstAcknowledgementAllowed = new CompletableFuture<>();
        TestAcknowledgeable firstAcknowledgeable = new TestAcknowledgeable("DATA", () -> {
            try {
                firstAcknowledgementStarted.complete(true);
                firstAcknowledgementAllowed.get();
            } catch (Exception e) {
                fail("Failed to block on second acknowledgement");
            }
        });

        AtomicInteger secondAcknowledgmentPreAllowedCount = new AtomicInteger(0);
        CompletableFuture<Boolean> secondAcknowledgerStarted = new CompletableFuture<>();
        CompletableFuture<Boolean> secondAcknowledgementAllowed = new CompletableFuture<>();
        AtomicInteger secondAcknowledgmentPostAllowedCount = new AtomicInteger(0);
        CompletableFuture<Boolean> secondAcknowledgerFinished = new CompletableFuture<>();
        TestAcknowledgeable secondAcknowledgeable = new TestAcknowledgeable("DATA", () -> {
            try {
                secondAcknowledgmentPreAllowedCount.incrementAndGet();
                secondAcknowledgerStarted.complete(true);
                secondAcknowledgementAllowed.get();
                secondAcknowledgmentPostAllowedCount.incrementAndGet();
                secondAcknowledgerFinished.complete(true);
            } catch (Exception e) {
                fail("Failed to block on second acknowledgement");
            }
        });

        FluxProcessor<TestAcknowledgeable, TestAcknowledgeable> upstream = EmitterProcessor.create(1);
        FluxSink<TestAcknowledgeable> sink = upstream.sink();

        List<Acknowledgeable<String>> emitted = new ArrayList<>();
        FluxProcessor<Acknowledgeable<String>, Acknowledgeable<String>> downstream = EmitterProcessor.create(1);
        downstream.subscribe(emitted::add);

        upstream.subscribe(new AcknowledgementQueuingSubscriber<>(downstream, String::length, OrderManagingAcknowledgementQueue::newWithImmediateErrors, 2));

        sink.next(firstAcknowledgeable);
        sink.next(secondAcknowledgeable);

        EXECUTOR.execute(() -> emitted.get(0).acknowledge());
        assertTrue(firstAcknowledgementStarted.get());

        // Erroneous implementation would block here indefinitely since first acknowledgement is still in-process (blocked)
        emitted.get(1).acknowledge();

        assertFalse(firstAcknowledgeable.isAcknowledged());
        assertFalse(secondAcknowledgeable.isAcknowledged());
        assertEquals(0, secondAcknowledgmentPreAllowedCount.get());
        assertFalse(secondAcknowledgerStarted.isDone());

        firstAcknowledgementAllowed.complete(true);
        assertTrue(secondAcknowledgerStarted.get());
        assertEquals(1, secondAcknowledgmentPreAllowedCount.get());
        assertTrue(firstAcknowledgeable.isAcknowledged());

        secondAcknowledgementAllowed.complete(true);

        Timing.waitForCondition(secondAcknowledgeable::isAcknowledged);

        assertTrue(secondAcknowledgeable.isAcknowledged());
        assertEquals(1, secondAcknowledgmentPreAllowedCount.get());
        assertEquals(1, secondAcknowledgmentPostAllowedCount.get());
    }

    @Test
    public void acknowledgeableEmissionsAreBoundedInQueuingOrder() {
        TestAcknowledgeable mom = new TestAcknowledgeable("MOM");
        TestAcknowledgeable dad = new TestAcknowledgeable("DAD");
        TestAcknowledgeable dog = new TestAcknowledgeable("DOG");
        TestAcknowledgeable cat = new TestAcknowledgeable("CAT");
        TestAcknowledgeable boy = new TestAcknowledgeable("BOY");
        Collection<TestAcknowledgeable> all = Arrays.asList(mom, dad, dog, cat, boy);

        FluxProcessor<TestAcknowledgeable, TestAcknowledgeable> upstream = EmitterProcessor.create(3);
        FluxSink<TestAcknowledgeable> sink = upstream.sink();

        List<Acknowledgeable<String>> emitted = new ArrayList<>();
        FluxProcessor<Acknowledgeable<String>, Acknowledgeable<String>> downstream = EmitterProcessor.create(1);
        downstream.subscribe(emitted::add);

        upstream.subscribe(new AcknowledgementQueuingSubscriber<>(downstream, String::length, OrderManagingAcknowledgementQueue::newWithImmediateErrors, 2));

        sink.next(mom);
        sink.next(dad);

        assertTrue(all.stream().noneMatch(TestAcknowledgeable::isAcknowledged));
        assertTrue(all.stream().noneMatch(TestAcknowledgeable::isNacknowledged));
        assertEquals(2, emitted.size());

        sink.next(dog);

        assertTrue(all.stream().noneMatch(TestAcknowledgeable::isAcknowledged));
        assertTrue(all.stream().noneMatch(TestAcknowledgeable::isNacknowledged));
        assertEquals(2, emitted.size());

        sink.next(cat);
        sink.next(boy);

        assertTrue(all.stream().noneMatch(TestAcknowledgeable::isAcknowledged));
        assertTrue(all.stream().noneMatch(TestAcknowledgeable::isNacknowledged));
        assertEquals(2, emitted.size());

        emitted.get(1).acknowledge();

        assertTrue(all.stream().noneMatch(TestAcknowledgeable::isAcknowledged));
        assertTrue(all.stream().noneMatch(TestAcknowledgeable::isNacknowledged));
        assertEquals(2, emitted.size());

        emitted.get(0).acknowledge();

        assertTrue(mom.isAcknowledged());
        assertTrue(dad.isAcknowledged());
        assertFalse(dog.isAcknowledged());
        assertFalse(cat.isAcknowledged());
        assertFalse(boy.isAcknowledged());
        assertEquals(4, emitted.size());

        emitted.get(2).acknowledge();

        assertTrue(mom.isAcknowledged());
        assertTrue(dad.isAcknowledged());
        assertTrue(dog.isAcknowledged());
        assertFalse(cat.isAcknowledged());
        assertFalse(boy.isAcknowledged());
        assertEquals(5, emitted.size());
    }

    @Test
    public void acknowledgementIsQueuedOnAPerGroupBasis() {
        TestAcknowledgeable mom = new TestAcknowledgeable("MOM");
        TestAcknowledgeable girl = new TestAcknowledgeable("GIRL");
        TestAcknowledgeable dad = new TestAcknowledgeable("DAD");
        TestAcknowledgeable yeet = new TestAcknowledgeable("YEET");
        Collection<TestAcknowledgeable> all = Arrays.asList(mom, girl, dad, yeet);

        FluxProcessor<TestAcknowledgeable, TestAcknowledgeable> upstream = EmitterProcessor.create(1);
        FluxSink<TestAcknowledgeable> sink = upstream.sink();

        List<Acknowledgeable<String>> emitted = new ArrayList<>();
        FluxProcessor<Acknowledgeable<String>, Acknowledgeable<String>> downstream = EmitterProcessor.create(1);
        downstream.subscribe(emitted::add);

        upstream.subscribe(new AcknowledgementQueuingSubscriber<>(downstream, String::length, OrderManagingAcknowledgementQueue::newWithImmediateErrors, 3));

        all.forEach(sink::next);

        assertEquals(3, emitted.size());

        emitted.get(2).acknowledge();

        assertTrue(all.stream().noneMatch(TestAcknowledgeable::isAcknowledged));

        emitted.get(0).acknowledge();

        assertTrue(mom.isAcknowledged());
        assertFalse(girl.isAcknowledged());
        assertTrue(dad.isAcknowledged());
        assertFalse(yeet.isAcknowledged());
        assertEquals(4, emitted.size());

        emitted.get(1).acknowledge();

        assertTrue(mom.isAcknowledged());
        assertTrue(girl.isAcknowledged());
        assertTrue(dad.isAcknowledged());
        assertFalse(yeet.isAcknowledged());

        emitted.get(3).acknowledge();

        assertTrue(all.stream().allMatch(TestAcknowledgeable::isAcknowledged));
    }
}