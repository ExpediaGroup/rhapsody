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
import java.util.Objects;
import java.util.Optional;

/**
 * A Timeout can be used to control the Blocking and Error behaviors of terminal operations from
 * the perspective of publishing Threads. There are three aspects of this behavior to control:
 * 1) Non-Blocking vs. Blocking (empty vs. non-empty Timeout)
 * 2) When Blocking, how long before timing out (magnitude of Timeout, with zero being indefinite)
 * 3) When Blocking, Error-ignoring vs. Error-throwing (negative vs. non-negative Timeout)
 * There are therefore the following behavior recommendations for a Timeout:
 * - `empty`    : Non-blocking consumption
 * - `negative` : Block for magnitude of timeout, ignore Errors
 * - `zero`     : Block indefinitely, throw Errors
 * - `positive` : Block for magnitude of timeout, throw Errors
 */
public final class Timeout {

    public static final String EMPTY = "EMPTY";

    private final Duration duration;

    private Timeout(Duration duration) {
        this.duration = duration;
    }

    public static Timeout parse(String timeout) {
        String nonNullTimeout = Objects.requireNonNull(timeout, "For parsing null Timeouts, use Timeout::EMPTY");
        return Objects.equals(EMPTY, nonNullTimeout) ? empty() : fromDuration(Duration.parse(nonNullTimeout));
    }

    public static Timeout empty() {
        return new Timeout(null);
    }

    public static Timeout fromDuration(Duration duration) {
        return new Timeout(Objects.requireNonNull(duration, "For null Timeout Durations, use Timeout::empty"));
    }

    public Optional<Duration> toDuration() {
        return Optional.ofNullable(duration);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        Timeout timeout = (Timeout) o;
        return Objects.equals(duration, timeout.duration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(duration);
    }

    @Override
    public String toString() {
        return Objects.toString(duration, EMPTY);
    }
}
