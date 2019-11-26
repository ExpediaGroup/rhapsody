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

import java.io.ByteArrayOutputStream;
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.junit.Test;

import com.expedia.rhapsody.kafka.serde.TestData;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AvroUpgradeTest {

    @Test // Upgrading from 1.8.2
    public void nullableMapWithBackwardCompatibleValueChangesCanBeDeserialized() throws Exception {
        TestDataWithNullableMap data = new TestDataWithNullableMap();
        data.setMap(Collections.singletonMap("KEY", TestData.create()));

        Schema writerSchema = ReflectData.get().getSchema(TestDataWithNullableMap.class);

        Schema readerMapSchema = Schema.createMap(Schema.createRecord(TestData.class.getName(), null, null, false,
            Collections.singletonList(new Schema.Field("data1", Schema.create(Schema.Type.STRING), null, Object.class.cast(null)))));
        Schema readerSchema = Schema.createRecord(TestDataWithNullableMap.class.getName(), null, null, false,
            Collections.singletonList(new Schema.Field("map", ReflectData.makeNullable(readerMapSchema), null, Object.class.cast(null))));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        new ReflectDatumWriter<>(writerSchema, ReflectData.get()).write(data, EncoderFactory.get().directBinaryEncoder(outputStream, null));

        byte[] serialized = outputStream.toByteArray();

        TestDataWithNullableMap deserialized = new ReflectDecoderAvroDeserializer.ReflectDecoderDatumReader<TestDataWithNullableMap>(writerSchema, readerSchema, ReflectData.get())
            .read(null, DecoderFactory.get().binaryDecoder(serialized, 0, serialized.length, null));

        assertEquals(data.getMap().get("KEY").getData1(), deserialized.getMap().get("KEY").getData1());
    }

    @Test // Upgrading from 1.8.2
    public void beanShouldBeSerializableAsAvroBinaryAndDeserializable() throws Exception {
        TestData testBean = TestData.create();

        AvroMapper avroMapper = new AvroMapper();

        AvroSchema avroSchema = avroMapper.schemaFor(TestData.class);

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        avroMapper.writer().with(avroSchema).writeValue(outputStream, testBean);

        byte[] serialized = outputStream.toByteArray();

        assertNotNull(serialized);
        assertTrue(serialized.length > 0);

        TestData deserialized = avroMapper.readerFor(TestData.class).with(avroSchema).readValue(serialized);

        assertEquals(testBean, deserialized);
    }
}
