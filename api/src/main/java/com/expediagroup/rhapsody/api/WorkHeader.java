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

import java.util.Optional;

/**
 * Provides metadata about work to be tried
 */
public interface WorkHeader {

    /**
     * Create a WorkHeader resulting from "recycling" this WorkHeader such that Work can be retried
     * in the future
     *
     * @param workType The type of Work to create for recycling. Usually WorkType.RECYCLE
     * @param cause A hint at what has caused recycling of the associated Work
     * @return A WorkHeader for Work to be recycled
     */
    WorkHeader recycle(WorkType workType, String cause);

    /**
     * The type of Work to be tried
     */
    WorkType type();

    /**
     * Should be used to track/identify associated units of Work
     */
    String marker();

    /**
     * Identifying information about the operand to apply Work to
     */
    String subject();

    /**
     * When the corresponding unit of Work was incepted
     */
    long inceptionEpochMilli();

    /**
     * An optional hint at what took place to precipitate requirement of Work
     */
    default Optional<String> event() {
        return Optional.empty();
    }

    /**
     * If recycling is tracked, how many times this Work has been recycled. Should be greater-than-
     * or-equal-to the number of times this Work has been tried.
     */
    default long recycleCount() {
        return -1;
    }
}
