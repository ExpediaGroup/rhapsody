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
package com.expedia.rhapsody.core.acknowledgement;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import com.expedia.rhapsody.util.Timing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class OrderManagingAcknowledgementQueueTest {

    @Test
    public void acknowledgementsAreExecutedInOrderOfCreationAfterCompletion() {
        AcknowledgementQueue queue = OrderManagingAcknowledgementQueue.newWithInOrderErrors();

        AtomicBoolean firstAcknowledged = new AtomicBoolean();
        AtomicBoolean secondAcknowledged = new AtomicBoolean();
        AtomicBoolean thirdAcknowledged = new AtomicBoolean();

        AcknowledgementQueue.InFlight firstInFlight = queue.add(() -> firstAcknowledged.set(true), error -> {});
        AcknowledgementQueue.InFlight secondInFlight = queue.add(() -> secondAcknowledged.set(true), error -> {});
        queue.add(() -> thirdAcknowledged.set(true), error -> {});

        long drained = queue.complete(secondInFlight);

        assertEquals(0L, drained);
        assertFalse(firstAcknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertFalse(thirdAcknowledged.get());

        drained = queue.complete(firstInFlight);

        assertEquals(2L, drained);
        assertTrue(firstAcknowledged.get());
        assertTrue(secondAcknowledged.get());
        assertFalse(thirdAcknowledged.get());
    }

    @Test
    public void nacknowledgementCanBeCompletedInOrder() {
        AcknowledgementQueue queue = OrderManagingAcknowledgementQueue.newWithInOrderErrors();

        AtomicBoolean firstAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> firstNacknowledged = new AtomicReference<>();
        AtomicBoolean secondAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> secondNacknowledged = new AtomicReference<>();

        AcknowledgementQueue.InFlight firstInFlight = queue.add(() -> firstAcknowledged.set(true), firstNacknowledged::set);
        AcknowledgementQueue.InFlight secondInFlight = queue.add(() -> secondAcknowledged.set(true), secondNacknowledged::set);

        long drained = queue.completeExceptionally(secondInFlight, new IllegalStateException());

        assertEquals(0L, drained);
        assertFalse(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertNull(secondNacknowledged.get());

        drained = queue.complete(firstInFlight);

        assertEquals(2L, drained);
        assertTrue(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);
    }

    @Test
    public void nacknowledgementCanBeExecutedImmediately() {
        AcknowledgementQueue queue = OrderManagingAcknowledgementQueue.newWithImmediateErrors();

        AtomicBoolean firstAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> firstNacknowledged = new AtomicReference<>();
        AtomicBoolean secondAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> secondNacknowledged = new AtomicReference<>();

        AcknowledgementQueue.InFlight firstInFlight = queue.add(() -> firstAcknowledged.set(true), firstNacknowledged::set);
        AcknowledgementQueue.InFlight secondInFlight = queue.add(() -> secondAcknowledged.set(true), secondNacknowledged::set);

        long drained = queue.completeExceptionally(secondInFlight, new IllegalStateException());

        assertEquals(0L, drained);
        assertFalse(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);

        drained = queue.complete(secondInFlight);

        assertEquals(0L, drained);
        assertFalse(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);

        drained = queue.complete(firstInFlight);

        assertEquals(2L, drained);
        assertTrue(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);
    }

    @Test
    public void recompletionOfInFlightsIsIgnored() {
        AcknowledgementQueue queue = OrderManagingAcknowledgementQueue.newWithInOrderErrors();

        AtomicInteger firstAcknowledgements = new AtomicInteger();
        AtomicReference<Throwable> firstNacknowledged = new AtomicReference<>();
        AtomicInteger secondAcknowledgements = new AtomicInteger();
        AtomicReference<Throwable> secondNacknowledged = new AtomicReference<>();

        AcknowledgementQueue.InFlight firstInFlight = queue.add(firstAcknowledgements::incrementAndGet, firstNacknowledged::set);
        AcknowledgementQueue.InFlight secondInFlight = queue.add(secondAcknowledgements::incrementAndGet, secondNacknowledged::set);

        queue.completeExceptionally(secondInFlight, new IllegalStateException());

        assertEquals(0, firstAcknowledgements.get());
        assertNull(firstNacknowledged.get());
        assertEquals(0, secondAcknowledgements.get());
        assertNull(secondNacknowledged.get());

        queue.complete(secondInFlight);

        assertEquals(0, firstAcknowledgements.get());
        assertNull(firstNacknowledged.get());
        assertEquals(0, secondAcknowledgements.get());
        assertNull(secondNacknowledged.get());

        queue.complete(firstInFlight);

        assertEquals(1, firstAcknowledgements.get());
        assertNull(firstNacknowledged.get());
        assertEquals(0, secondAcknowledgements.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);

        queue.complete(firstInFlight);

        assertEquals(1, firstAcknowledgements.get());
        assertNull(firstNacknowledged.get());
        assertEquals(0, secondAcknowledgements.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);

        queue.complete(secondInFlight);

        assertEquals(1, firstAcknowledgements.get());
        assertNull(firstNacknowledged.get());
        assertEquals(0, secondAcknowledgements.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);
    }

    @Test
    public void executionOnlyHappensOnOneThreadInNonBlockingFashion() throws Exception {
        CompletableFuture<Boolean> firstAcknowledgementStarted = new CompletableFuture<>();
        CompletableFuture<Boolean> firstAcknowledgementAllowed = new CompletableFuture<>();
        Runnable firstAcknowledger = () -> {
            try {
                firstAcknowledgementStarted.complete(true);
                firstAcknowledgementAllowed.get();
            } catch (Exception e) {
                fail("This should never happen");
            }
        };

        CompletableFuture<Boolean> secondAcknowledgementStarted = new CompletableFuture<>();
        Runnable secondAcknowledger = () -> secondAcknowledgementStarted.complete(true);

        AcknowledgementQueue queue = OrderManagingAcknowledgementQueue.newWithInOrderErrors();

        AcknowledgementQueue.InFlight firstInFlight = queue.add(firstAcknowledger, error -> {});
        AcknowledgementQueue.InFlight secondInFlight = queue.add(secondAcknowledger, error -> {});

        AtomicLong asyncDrained = new AtomicLong(0L);
        Executors.newSingleThreadExecutor().submit(() -> asyncDrained.set(queue.complete(firstInFlight)));

        assertTrue(firstAcknowledgementStarted.get());

        // Would block indefinitely here if execution blocked since first execution in progress
        long syncDrained = queue.complete(secondInFlight);

        assertEquals(0, syncDrained);

        firstAcknowledgementAllowed.complete(true);

        assertTrue(secondAcknowledgementStarted.get());

        Timing.waitForCondition(() -> asyncDrained.get() != 0);

        assertEquals(2, asyncDrained.get());
    }
}