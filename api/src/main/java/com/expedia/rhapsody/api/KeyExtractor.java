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
 * Extracts an identifying or near-identifying attribute of data items. In contrast to
 * {@link GroupExtractor GroupExtrator}, KeyExtractors should extract much finer-grained attributes
 * of data items. For instance, this may be a Message's ID, or the ID of an Entity to which this
 * data item is related. This is useful for abstractly implementing how to extract Record keys
 * or Entity primary keys.
 *
 * @param <T> The type of data item to extract a key from
 */
public interface KeyExtractor<T> extends Function<T, Object> {

}
