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
package com.expedia.rhapsody.kafka.avro.util;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;

import com.expedia.rhapsody.util.TypeResolution;

public final class AvroSchemas {

    private AvroSchemas() {

    }

    public static Schema getOrReflectNullable(Object data) {
        return makeNullable(getOrReflect(data));
    }

    public static Schema getOrReflect(Object data) {
        return getOrSupply(data, () -> ReflectData.get().getSchema(data.getClass()));
    }

    public static Schema getOrSupply(Object data, Supplier<Schema> schemaSupplier) {
        try {
            return data instanceof GenericContainer ? GenericContainer.class.cast(data).getSchema() :
                SpecificData.get().getSchema(TypeResolution.safelyGetClass(data));
        } catch (AvroRuntimeException e) {
            return schemaSupplier.get();
        }
    }

    public static Schema makeNullable(Schema schema) {
        return isNull(schema) ? schema : ReflectData.makeNullable(schema);
    }

    public static Optional<Schema> reduceNonNull(Schema schema) {
        if (isUnion(schema)) {
            return schema.getTypes().stream().reduce((schema1, schema2) -> isNull(schema1) ? schema2 : schema1);
        } else {
            return isNull(schema) ? Optional.empty() : Optional.of(schema);
        }
    }

    public static boolean isNullable(Schema schema) {
        return isUnion(schema) && schema.getTypes().stream().anyMatch(AvroSchemas::isNull);
    }

    public static boolean isUnion(Schema schema) {
        return schema.getType() == Schema.Type.UNION;
    }

    public static boolean isRecord(Schema schema) {
        return schema.getType() == Schema.Type.RECORD;
    }

    public static boolean isNull(Schema schema) {
        return schema.getType() == Schema.Type.NULL;
    }
}
