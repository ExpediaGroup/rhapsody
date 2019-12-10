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
package com.expediagroup.rhapsody.kafka.avro.serde;

import org.junit.Test;

import com.expediagroup.rhapsody.kafka.serde.AbstractSerDeTest;
import com.expediagroup.rhapsody.kafka.serde.TestData;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ReflectAvroSerDeTest extends LoadingAvroSerDeTest {

    public ReflectAvroSerDeTest() {
        super(TestReflectEncoderAvroSerializer::new, TestReflectDecoderAvroDeserializer::new);
    }

    @Test
    public void recursivelyGenericDataCanBeSerializedAndDeserialized() {
        String data = "DATA";

        TestGenericData<String> genericData = new TestGenericData<>();
        genericData.setData(data);

        TestGenericData<TestGenericData<String>> genericGenericData = new TestGenericData<>();
        genericGenericData.setData(genericData);

        byte[] serializedData = serializer.serialize(AbstractSerDeTest.TOPIC, genericGenericData);

        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        TestGenericData deserialized = (TestGenericData) deserializer.deserialize(AbstractSerDeTest.TOPIC, serializedData);

        assertEquals(genericGenericData, deserialized);
    }

    @Test // Note that this test is mainly pertinent to Avro >= 1.9.0
    public void nullableGenericDataWithAddedFieldCanBeDeserialized() {
        TestData data = TestData.create();

        GenericDataHolderWithAdditionalField<TestData> genericData = new GenericDataHolderWithAdditionalField<>();
        genericData.setData(data);
        genericData.setExtraData("extraData");

        byte[] serializedData = serializer.serialize(TOPIC, genericData);

        assertNotNull(serializedData);
        assertTrue(serializedData.length > 0);

        GenericDataHolder deserialized = (GenericDataHolder) deserializer.deserialize(TOPIC, serializedData);

        assertEquals(data, deserialized.getData());
    }
}
