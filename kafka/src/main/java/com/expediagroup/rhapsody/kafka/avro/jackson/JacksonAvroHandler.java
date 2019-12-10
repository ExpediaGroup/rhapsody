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
package com.expediagroup.rhapsody.kafka.avro.jackson;

import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.Nullable;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class JacksonAvroHandler {

    private JacksonAvroHandler() {

    }

    public static AvroMapper createAvroMapper() {
        AvroMapper avroMapper = new AvroMapper(createAvroFactory());
        avroMapper.enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN);
        avroMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        avroMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        avroMapper.registerModule(new JavaTimeModule());
        avroMapper.setAnnotationIntrospector(new AvroAnnotationIntrospector());
        return avroMapper;
    }

    public static AvroFactory createAvroFactory() {
        AvroFactory avroFactory = new AvroFactory();
        avroFactory.enable(JsonGenerator.Feature.IGNORE_UNKNOWN);
        return avroFactory;
    }

    public static final class AvroAnnotationIntrospector extends JacksonAnnotationIntrospector {

        @Override
        public boolean hasIgnoreMarker(AnnotatedMember m) {
            return m.hasAnnotation(AvroIgnore.class) || super.hasIgnoreMarker(m);
        }

        @Override
        public Boolean hasRequiredMarker(AnnotatedMember m) {
            return !m.hasAnnotation(Nullable.class) || Boolean.TRUE.equals(super.hasRequiredMarker(m)); // Guard against null Boolean
        }
    }
}
