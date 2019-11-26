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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

public class GenericRecordAvroSerializer extends AvroSerDe implements Serializer<GenericRecord> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericRecordAvroSerializer.class);

    private boolean isKey = false;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.configureClientProperties(new KafkaAvroSerializerConfig(configs));
        this.isKey = isKey;
    }

    @Override
    public byte[] serialize(String topic, GenericRecord data) {
        // null needs to treated specially since the client most likely just wants to send
        // an individual null value instead of making the subject a null type. Also, null in
        // Kafka has a special meaning for deletion in a topic with the compact retention policy.
        // Therefore, we will bypass schema handling and return a null value in Kafka, instead
        // of an Avro encoded null.
        try {
            return data == null ? null : serializeNonNull(topic, data);
        } catch (RestClientException e) {
            LOGGER.warn("Error registering Avro Schema", e);
            throw new SerializationException("Error registering Avro Schema", e);
        } catch (IOException | RuntimeException e) {
            // Avro can throw AvroRuntimeException, NullPointerException, ClassCastException, etc
            LOGGER.warn("Error serializing Avro message", e);
            throw new SerializationException("Error serializing Avro message", e);
        }
    }

    @Override
    public void close() {

    }

    public byte[] serializeNonNull(String topic, GenericRecord data) throws RestClientException, IOException {
        String subject = getSubjectName(topic, isKey, data, data.getSchema());
        int schemaId = register(subject, data.getSchema());
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        output.write(AbstractKafkaAvroSerDe.MAGIC_BYTE);
        output.write(ByteBuffer.allocate(AbstractKafkaAvroSerDe.idSize).putInt(schemaId).array());
        new GenericDatumWriter<>(data.getSchema()).write(data, EncoderFactory.get().directBinaryEncoder(output, null));
        return output.toByteArray();
    }
}
