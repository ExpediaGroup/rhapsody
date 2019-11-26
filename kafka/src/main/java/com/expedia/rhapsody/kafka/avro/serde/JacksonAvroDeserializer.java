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

import java.io.IOException;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.kafka.common.serialization.Deserializer;

import com.expedia.rhapsody.kafka.avro.jackson.JacksonAvroHandler;
import com.expedia.rhapsody.kafka.avro.util.AvroDeserialization;
import com.expedia.rhapsody.kafka.avro.util.AvroSchemas;
import com.expedia.rhapsody.util.Collecting;
import com.expedia.rhapsody.util.Instantiation;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;

public class JacksonAvroDeserializer<T> extends LoadingAvroDeserializer<T> implements Deserializer<T> {

    private static final Map<Schema, JavaType> JAVA_TYPES_BY_READER_SCHEMA = new ConcurrentHashMap<>();

    private final AvroMapper avroMapper;

    public JacksonAvroDeserializer() {
        this(JacksonAvroHandler.createAvroMapper());
    }

    public JacksonAvroDeserializer(AvroMapper avroMapper) {
        this.avroMapper = avroMapper;
    }

    @Override
    protected Schema loadTypeSchema(Type dataType) {
        try {
            return avroMapper.schemaFor(avroMapper.constructType(dataType)).getAvroSchema();
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not generate Avro Schema for data Type: " + dataType, e);
        }
    }

    @Override
    protected T deserializeNonNullWithSchemas(Schema writerSchema, Schema readerSchema, ByteBuffer dataBuffer) throws IOException {
        return dataBuffer.remaining() <= 0 ?
            Instantiation.one((Class<? extends T>) ReflectData.get().getClass(readerSchema)) :
            deserializeNonEmptyWithSchema(readerSchema, dataBuffer);
    }

    private T deserializeNonEmptyWithSchema(Schema readerSchema, ByteBuffer dataBuffer) throws IOException {
        return avroMapper.readerFor(JAVA_TYPES_BY_READER_SCHEMA.computeIfAbsent(readerSchema, this::constructSchemaType))
            .with(new AvroSchema(readerSchema))
            .readValue(dataBuffer.array(), dataBuffer.arrayOffset() + dataBuffer.position(), dataBuffer.remaining());
    }

    private JavaType constructSchemaType(Schema schema) {
        Class schemaClass = ReflectData.get().getClass(schema);

        // Note usage of LinkedHashMap to keep TypeVariables in declaration order
        JavaType defaultType = avroMapper.constructType(Object.class);
        Map<TypeVariable, JavaType> javaTypesByVariable = Collecting.toMap(schemaClass.getTypeParameters(), Function.identity(), typeVariable -> defaultType, LinkedHashMap::new);

        Map<String, TypeVariable> typeVariablesByName = javaTypesByVariable.keySet().stream().collect(Collectors.toMap(TypeVariable::getName, Function.identity()));
        AvroSchemas.reduceNonNull(schema).ifPresent(nonNullSchema ->
            AvroDeserialization.consumeGenericFields(schemaClass, nonNullSchema, (genericField, fieldSchema) ->
                javaTypesByVariable.computeIfPresent(typeVariablesByName.get(genericField.getGenericType().getTypeName()),
                    (typeVariable, currentType) -> currentType == defaultType ? constructSchemaType(fieldSchema) : currentType)));

        return avroMapper.getTypeFactory().constructParametricType(schemaClass, javaTypesByVariable.values().toArray(new JavaType[javaTypesByVariable.size()]));
    }
}