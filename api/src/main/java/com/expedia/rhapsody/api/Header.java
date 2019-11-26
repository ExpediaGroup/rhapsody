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

import java.util.Collections;
import java.util.Map;

/**
 * A data item may contain metadata associated with it that is not strictly a subset of information
 * contained in the data item. Such information can be propagated on {@link Headed Headed} data
 * items in a Header.
 */
@FunctionalInterface
public interface Header {

    static Header empty() {
        return Collections::emptyMap;
    }

    static Header fromMap(Map<String, String> map) {
        return () -> map;
    }

    /**
     * Convert this Header to a String-to-String Map of attributes
     */
    Map<String, String> toMap();
}
