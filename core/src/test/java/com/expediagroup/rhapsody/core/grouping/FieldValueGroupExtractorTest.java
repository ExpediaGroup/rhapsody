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

import java.util.function.Function;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FieldValueGroupExtractorTest {

    @Test
    public void groupCanBeExtractedFromField() {
        TestData data = new TestData("GROUP");

        Function<TestData, Object> groupExtractor = new FieldValueGroupExtractor<>("group");

        assertEquals(data.getGroup(), groupExtractor.apply(data));
    }

    public static final class TestData {

        private final String group;

        public TestData(String group) {
            this.group = group;
        }

        public String getGroup() {
            return group;
        }
    }
}