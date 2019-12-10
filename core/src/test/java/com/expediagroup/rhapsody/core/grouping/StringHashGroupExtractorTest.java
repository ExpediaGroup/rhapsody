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
package com.expediagroup.rhapsody.core.grouping;

import org.junit.Test;

import com.expediagroup.rhapsody.api.GroupExtractor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class StringHashGroupExtractorTest {

    @Test
    public void stringsAreAppropriatelyHashed() {
        String string1 = "Hello, World";
        String string2 = "Hola, Mundo";

        GroupExtractor<String> groupExtractor = new TestStringHashGroupExtractor(2);

        assertEquals(groupExtractor.apply(string1), groupExtractor.apply(string1));
        assertNotEquals(groupExtractor.apply(string1), groupExtractor.apply(string2));
    }

    private static final class TestStringHashGroupExtractor extends StringHashGroupExtractor<String> {

        public TestStringHashGroupExtractor(int modulus) {
            super(modulus);
        }

        @Override
        protected String extractString(String s) {
            return s;
        }
    }
}