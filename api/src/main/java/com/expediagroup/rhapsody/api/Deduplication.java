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

/**
 * Configures how to deduplicate data items.
 *
 * @param <T> The type of data item being deduplicated
 */
@FunctionalInterface
public interface Deduplication<T> {

    /**
     * Returns the key on which to implement deduplication
     *
     * @param t The data item that deduplication is being enforced on
     * @return Some Object representing a "key" on which to base deduplication
     */
    Object extractKey(T t);

    /**
     * Upon occurrence of duplication, apply a reduction to produce a deduplicated result
     *
     * @return A reduction of two items deemed to be duplicates (have same key)
     */
    default T reduceDuplicates(T t1, T t2) {
        return t1;
    }
}
