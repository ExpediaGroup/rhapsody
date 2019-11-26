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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MultipleAcknowledgementQueueTest {

    @Test
    public void acknowledgementAppliesUpToAndIncludingAllPreviousInFlightAcknowledgements() {
        AcknowledgementQueue queue = new MultipleAcknowledgementQueue();

        AtomicBoolean firstAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> firstNacknowledged = new AtomicReference<>();
        AtomicBoolean secondAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> secondNacknowledged = new AtomicReference<>();
        AtomicBoolean thirdAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> thirdNacknowledged = new AtomicReference<>();
        AtomicBoolean fourthAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> fourthNacknowledged = new AtomicReference<>();
        AtomicBoolean fifthAcknowledged = new AtomicBoolean();
        AtomicReference<Throwable> fifthNacknowledged = new AtomicReference<>();

        AcknowledgementQueue.InFlight firstInFlight = queue.add(() -> firstAcknowledged.set(true), firstNacknowledged::set);
        AcknowledgementQueue.InFlight secondInFlight = queue.add(() -> secondAcknowledged.set(true), secondNacknowledged::set);
        AcknowledgementQueue.InFlight thirdInFlight = queue.add(() -> thirdAcknowledged.set(true), thirdNacknowledged::set);
        queue.add(() -> fourthAcknowledged.set(true), fourthNacknowledged::set);
        AcknowledgementQueue.InFlight fifthInFlight = queue.add(() -> fifthAcknowledged.set(true), fifthNacknowledged::set);

        long drained = queue.complete(firstInFlight);

        assertEquals(1L, drained);
        assertTrue(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertNull(secondNacknowledged.get());
        assertFalse(thirdAcknowledged.get());
        assertNull(thirdNacknowledged.get());
        assertFalse(fourthAcknowledged.get());
        assertNull(fourthNacknowledged.get());
        assertFalse(fifthAcknowledged.get());
        assertNull(fifthNacknowledged.get());

        drained = queue.completeExceptionally(thirdInFlight, new IllegalStateException());

        assertEquals(2L, drained);
        assertTrue(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);
        assertFalse(thirdAcknowledged.get());
        assertTrue(thirdNacknowledged.get() instanceof IllegalStateException);
        assertFalse(fourthAcknowledged.get());
        assertNull(fourthNacknowledged.get());
        assertFalse(fifthAcknowledged.get());
        assertNull(fifthNacknowledged.get());

        // Duplicate completion should be ignored
        drained = queue.complete(secondInFlight) + queue.completeExceptionally(thirdInFlight, new IllegalArgumentException());

        assertEquals(0L, drained);
        assertTrue(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);
        assertFalse(thirdAcknowledged.get());
        assertTrue(thirdNacknowledged.get() instanceof IllegalStateException);
        assertFalse(fourthAcknowledged.get());
        assertNull(fourthNacknowledged.get());
        assertFalse(fifthAcknowledged.get());
        assertNull(fifthNacknowledged.get());

        drained = queue.complete(fifthInFlight);

        assertEquals(2L, drained);
        assertTrue(firstAcknowledged.get());
        assertNull(firstNacknowledged.get());
        assertFalse(secondAcknowledged.get());
        assertTrue(secondNacknowledged.get() instanceof IllegalStateException);
        assertFalse(thirdAcknowledged.get());
        assertTrue(thirdNacknowledged.get() instanceof IllegalStateException);
        assertTrue(fourthAcknowledged.get());
        assertNull(fourthNacknowledged.get());
        assertTrue(fifthAcknowledged.get());
        assertNull(fifthNacknowledged.get());
    }
}