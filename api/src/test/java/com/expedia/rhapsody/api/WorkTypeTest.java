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
import java.util.stream.Stream;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WorkTypeTest {

    @Test
    public void workTypesOfHighestRelevanceAreChosen() {
        assertEquals(WorkType.INTENT, Stream.<WorkType>empty().collect(WorkType.highestRelevanceReducing(Function.identity())));

        assertEquals(WorkType.COMMIT, Stream.of(WorkType.COMMIT).collect(WorkType.highestRelevanceReducing(Function.identity())));
        assertEquals(WorkType.RETRY, Stream.of(WorkType.RETRY).collect(WorkType.highestRelevanceReducing(Function.identity())));
        assertEquals(WorkType.CANCEL, Stream.of(WorkType.CANCEL).collect(WorkType.highestRelevanceReducing(Function.identity())));

        assertEquals(WorkType.COMMIT, Stream.of(WorkType.COMMIT, WorkType.CANCEL).collect(WorkType.highestRelevanceReducing(Function.identity())));
        assertEquals(WorkType.COMMIT, Stream.of(WorkType.COMMIT, WorkType.RETRY).collect(WorkType.highestRelevanceReducing(Function.identity())));
        assertEquals(WorkType.COMMIT, Stream.of(WorkType.COMMIT, WorkType.INTENT).collect(WorkType.highestRelevanceReducing(Function.identity())));
        assertEquals(WorkType.CANCEL, Stream.of(WorkType.RETRY, WorkType.CANCEL).collect(WorkType.highestRelevanceReducing(Function.identity())));
        assertEquals(WorkType.CANCEL, Stream.of(WorkType.INTENT, WorkType.CANCEL).collect(WorkType.highestRelevanceReducing(Function.identity())));
        assertEquals(WorkType.RETRY, Stream.of(WorkType.RETRY, WorkType.INTENT).collect(WorkType.highestRelevanceReducing(Function.identity())));
    }
}