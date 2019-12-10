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
package com.expediagroup.rhapsody.kafka.avro.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.reflect.AvroIgnore;

import com.expediagroup.rhapsody.util.FieldResolution;
import com.expediagroup.rhapsody.util.TypeResolution;
import com.expediagroup.rhapsody.util.ValueResolution;

public final class AvroSerialization {

    private AvroSerialization() {

    }

    public static Schema generateWriterSchema(Object data, Function<Type, Schema> typeSchemaLoader) {
        return generateWriterSchema(data, data.getClass(), typeSchemaLoader);
    }

    private static Schema generateWriterSchema(Object data, Type type, Function<Type, Schema> typeSchemaLoader) {
        return TypeResolution.isGenericClass(data.getClass()) && !TypeResolution.isDataStructure(data.getClass()) ?
            Schema.createRecord(data.getClass().getCanonicalName(), null, null, false, generateWriterSchemaFields(data, typeSchemaLoader)) :
            typeSchemaLoader.apply(type instanceof TypeVariable ? data.getClass() : type);
    }

    private static List<Schema.Field> generateWriterSchemaFields(Object data, Function<Type, Schema> typeSchemaLoader) {
        return FieldResolution.getAllFields(data.getClass()).stream()
            .filter(AvroSerialization::shouldGenerateWriterSchemaField)
            .map(field -> generateWriterSchemaField(field, ValueResolution.getFieldValue(data, field), typeSchemaLoader))
            .filter(writerSchemaField -> !AvroSchemas.isNull(writerSchemaField.schema()))
            .collect(Collectors.toList());
    }

    private static boolean shouldGenerateWriterSchemaField(Field field) {
        return !Modifier.isStatic(field.getModifiers()) &&
            !Modifier.isTransient(field.getModifiers()) &&
            !field.isAnnotationPresent(AvroIgnore.class);
    }

    private static Schema.Field generateWriterSchemaField(Field field, Object value, Function<Type, Schema> typeSchemaLoader) {
        Schema fieldSchema = AvroSchemas.getOrSupply(value, () -> generateWriterSchema(value, field.getGenericType(), typeSchemaLoader));
        return new Schema.Field(field.getName(), fieldSchema, null, Object.class.cast(null));
    }
}
