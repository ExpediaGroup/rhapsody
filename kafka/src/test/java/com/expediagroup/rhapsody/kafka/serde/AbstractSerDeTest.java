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
package com.expediagroup.rhapsody.kafka.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public abstract class AbstractSerDeTest {

    protected static final String TOPIC = "topic";

    protected final Serializer serializer;

    protected final Deserializer deserializer;

    protected AbstractSerDeTest(Serializer serializer, Deserializer deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Test
    public void dataCanBeSerializedAndDeserialized() {
        TestData data = TestData.create();

        byte[] serializedData = serializer.serialize(TOPIC, data);

        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        TestData deserialized = (TestData) deserializer.deserialize(TOPIC, serializedData);

        assertEquals(data, deserialized);
    }
}
