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
package com.expediagroup.rhapsody.api;

import java.time.Duration;
import java.time.Instant;

/**
 * Defines Work that may be prepared to be done and metadata associated with it
 */
public interface Work {

    default Duration sinceInception() {
        return Duration.ofMillis(Instant.now().toEpochMilli() - workHeader().inceptionEpochMilli());
    }

    default boolean inceptedAfter(Work other) {
        return inceptedAfter(other.workHeader().inceptionEpochMilli());
    }

    default boolean inceptedAfter(Instant instant) {
        return inceptedAfter(instant.toEpochMilli());
    }

    default boolean inceptedAfter(long inceptionEpochMilli) {
        return workHeader().inceptionEpochMilli() > inceptionEpochMilli;
    }

    WorkHeader workHeader();

    boolean isPrepared();
}
