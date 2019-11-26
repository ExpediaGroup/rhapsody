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
package com.expedia.rhapsody.api;

import java.util.function.Function;

/**
 * Extracts an Object from data items representing the "group" that an item belongs to. In Contrast
 * to {@link KeyExtractor KeyExtractor}, implementations should extract "coarsely" grained
 * information about data items. For example, implementations may extract a data item's modulus of
 * a UUID. This is useful for such things as logical partitioning of data items in to processing
 * groups or other coarse categorization of data items
 *
 * @param <T> The type of data item being grouped
 */
public interface GroupExtractor<T> extends Function<T, Object> {

    static int validNumGroupsOrDefault(Integer numGroups) {
        return numGroups == null || numGroups <= 0 ? defaultNumGroups() : numGroups;
    }

    static int defaultNumGroups() {
        return Runtime.getRuntime().availableProcessors();
    }
}
