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
package com.expediagroup.rhapsody.util;

import java.time.Duration;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TimeoutTest {

    @Test
    public void emptyTimeoutReturnsEmptyDuration() {
        assertFalse(Timeout.empty().toDuration().isPresent());
        assertFalse(Timeout.parse(Timeout.EMPTY).toDuration().isPresent());
    }

    @Test
    public void timeoutCanBeCreatedFromDuration() {
        Duration expected = Duration.ofSeconds(1L);
        assertEquals(expected, Timeout.fromDuration(expected).toDuration().orElse(Duration.ZERO));
        assertEquals(expected.negated(), Timeout.fromDuration(expected).toDuration().orElse(Duration.ZERO).negated());
    }

    @Test
    public void timeoutCanBeParsed() {
        String duration = "P1DT1S";
        assertEquals(Duration.parse(duration), Timeout.fromDuration(Duration.parse(duration)).toDuration().orElse(Duration.ZERO));

        String negativeDuration = "P-1DT-1S";
        assertEquals(Duration.parse(negativeDuration), Timeout.fromDuration(Duration.parse(negativeDuration)).toDuration().orElse(Duration.ZERO));
    }
}