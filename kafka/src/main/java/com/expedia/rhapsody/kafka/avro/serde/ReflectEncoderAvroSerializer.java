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
import java.lang.reflect.Type;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import com.expedia.rhapsody.util.ConfigLoading;

public class ReflectEncoderAvroSerializer<T> extends LoadingAvroSerializer<T> {

    public static final String REFLECT_ALLOW_NULL_PROPERTY = "reflect.allow.null";

    private boolean reflectAllowNull = false;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        super.configure(configs, isKey);
        this.reflectAllowNull = ConfigLoading.load(configs, REFLECT_ALLOW_NULL_PROPERTY, Boolean::valueOf, reflectAllowNull);
    }

    @Override
    protected Schema loadTypeSchema(Type dataType) {
        return getReflectData().getSchema(dataType);
    }

    @Override
    protected void serializeDataToOutput(ByteArrayOutputStream output, Schema schema, T data) throws IOException {
        new ReflectDatumWriter<>(schema, getReflectData()).write(data, EncoderFactory.get().directBinaryEncoder(output, null));
    }

    private ReflectData getReflectData() {
        return reflectAllowNull ? ReflectData.AllowNull.get() : ReflectData.get();
    }
}
