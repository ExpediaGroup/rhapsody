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
package com.expedia.rhapsody.kafka.extractor;

import java.util.Collections;
import java.util.function.Function;

import org.junit.Test;

import com.expedia.rhapsody.api.GroupExtractor;
import com.expedia.rhapsody.core.grouping.FieldValueGroupExtractor;
import com.expedia.rhapsody.kafka.serde.TestData;

import static org.junit.Assert.assertEquals;

public class GroupTopicExtractorTest {

    @Test
    public void topicCanBeExtractedFromGrouping() {
        TestData data = TestData.create();

        GroupExtractor<TestData> groupExtractor = new FieldValueGroupExtractor<>("data1");
        Function<TestData, String> topicExtractor = new GroupTopicExtractor<>(groupExtractor, Collections.singletonMap(data.getData1(), "TOPIC"));

        assertEquals("TOPIC", topicExtractor.apply(data));
    }
}