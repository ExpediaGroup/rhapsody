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
package com.expediagroup.rhapsody.util;

import java.lang.reflect.Type;
import java.util.function.Supplier;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class JacksonHandler {

    private JacksonHandler() {

    }

    public static String convertToString(ObjectMapper objectMapper, Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not write JSON String from Object: " + object, e);
        }
    }

    public static <T> T convertFromString(ObjectMapper objectMapper, String string, Type toType) {
        try {
            return objectMapper.readValue(string, objectMapper.constructType(toType));
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Could not read JSON String into Object: %s %s", toType, string), e);
        }
    }

    public static ObjectMapper initializeObjectMapper(Supplier<ObjectMapper> objectMapperSupplier) {
        ObjectMapper objectMapper = objectMapperSupplier.get();

        // Only serialize Fields
        objectMapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.NONE);
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

        // In case anyone wants to override any Modules we register
        objectMapper.disable(MapperFeature.IGNORE_DUPLICATE_MODULE_REGISTRATIONS);

        // Forward Compatibility
        objectMapper.enable(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL);
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

        // Java 8 Time Support
        objectMapper.registerModule(new JavaTimeModule());

        // Write Dates as Strings
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // Solves BigDecimal Precision Stripping
        objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

        return objectMapper;
    }
}
