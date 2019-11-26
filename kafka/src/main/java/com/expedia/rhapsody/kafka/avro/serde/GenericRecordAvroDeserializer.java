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

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.serialization.Deserializer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

public class GenericRecordAvroDeserializer extends AvroSerDe implements Deserializer<GenericRecord> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.configureClientProperties(new KafkaAvroDeserializerConfig(configs));
    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
        return data == null || data.length == 0 ? null : deserializeNonNull(topic, data);
    }

    @Override
    public void close() {

    }

    protected GenericRecord deserializeNonNull(String topic, byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        validateByte(MAGIC_BYTE, buffer.get());
        int writerSchemaId = buffer.getInt();
        try {
            Schema schema = getById(writerSchemaId);
            Decoder decoder = DecoderFactory.get()
                .binaryDecoder(buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining(), null);
            return GenericRecord.class.cast(new GenericDatumReader<>(schema).read(null, decoder));
        } catch (RestClientException e) {
            throw new IllegalStateException("Failed to retrieve Schema for id: " + writerSchemaId, e);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to deserialize Avro message", e);
        }
    }
}
