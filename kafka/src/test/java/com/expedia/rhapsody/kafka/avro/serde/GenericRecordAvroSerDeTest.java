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
package com.expedia.rhapsody.kafka.avro.serde;

import java.util.Arrays;
import java.util.Objects;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.Test;

import com.expedia.rhapsody.kafka.serde.TestData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GenericRecordAvroSerDeTest {

    protected static final String TOPIC = "topic";

    private final TestSchemaRegistry registry = new TestSchemaRegistry();

    private final Serializer<TestData> reflectiveSerializer = new TestReflectEncoderAvroSerializer<>(registry);

    private final Deserializer<GenericRecord> genericRecordDeserializer = new TestGenericRecordAvroDeserializer(registry);

    private final Serializer<GenericRecord> genericRecordSerializer = new TestGenericRecordAvroSerializer(registry);

    private final Deserializer<TestData> reflectiveDeserializer = new TestReflectDecoderAvroDeserializer<>(registry);

    @Test
    public void dataCanBeDeserializedAsGenericRecord() {
        TestData data = TestData.create();

        byte[] serializedData = reflectiveSerializer.serialize(TOPIC, data);

        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        GenericRecord deserialized = genericRecordDeserializer.deserialize(TOPIC, serializedData);

        assertNotNull(deserialized);
        assertEquals(data.getData1(), Objects.toString(deserialized.get("data1")));
        assertEquals(data.getData2(), Objects.toString(deserialized.get("data2")));
    }

    @Test
    public void dataCanBeDeserializedAndReserializedAsGenericRecord() {
        TestData data = TestData.create();

        byte[] reflectivelySerialized = reflectiveSerializer.serialize(TOPIC, data);

        GenericRecord genericallyDeserialized = genericRecordDeserializer.deserialize(TOPIC, reflectivelySerialized);
        byte[] genericallySerialized = genericRecordSerializer.serialize(TOPIC, genericallyDeserialized);

        TestData result = reflectiveDeserializer.deserialize(TOPIC, genericallySerialized);

        assertFalse(Arrays.equals(reflectivelySerialized, genericallySerialized));
        assertNotNull(result);
        assertEquals(data, result);
    }
}
