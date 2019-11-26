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
package com.expedia.rhapsody.kafka.jackson.serde;

import java.util.Collections;

import org.junit.Before;

import com.expedia.rhapsody.kafka.serde.AbstractSerDeTest;
import com.expedia.rhapsody.kafka.serde.TestData;

public class JacksonSerDeTest extends AbstractSerDeTest {

    public JacksonSerDeTest() {
        super(new JacksonSerializer(), new JacksonDeserializer());
    }

    @Before
    public void setup() {
        deserializer.configure(Collections.singletonMap(JacksonDeserializer.VALUE_DESERIALIZATION_PROPERTY, TestData.class.getName()), false);
    }
}
