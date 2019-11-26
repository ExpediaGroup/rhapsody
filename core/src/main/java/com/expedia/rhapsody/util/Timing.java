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
package com.expedia.rhapsody.util;

import java.util.function.Supplier;

public final class Timing {

    private static final long DEFAULT_WAIT_MILLIS = Long.valueOf(System.getProperty("timing.defaultwaitmillis", "10000"));

    private static final long CONDITION_PAUSE_MILLIS = Long.valueOf(System.getProperty("timing.conditionpausemillis", "10"));

    private Timing() {

    }

    public static void waitForCondition(Supplier<Boolean> condition) {
        waitForCondition(condition, DEFAULT_WAIT_MILLIS);
    }

    public static void waitForCondition(Supplier<Boolean> condition, long maxWaitMillis) {
        long millisWaited = 0L;
        while (millisWaited < maxWaitMillis && !condition.get()) {
            pause(CONDITION_PAUSE_MILLIS);
            millisWaited += CONDITION_PAUSE_MILLIS;
        }
    }

    public static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception e) {
            System.err.println("Failed to pause: " + e);
        }
    }
}
