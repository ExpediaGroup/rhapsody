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
package com.expediagroup.rhapsody.kafka.jackson.serde;

import java.util.Collections;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.expediagroup.rhapsody.util.ConfigLoading;
import com.expediagroup.rhapsody.util.JacksonHandler;
import com.expediagroup.rhapsody.util.TypeResolution;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonDeserializer<T> implements Deserializer<T> {

    public static final String VALUE_DESERIALIZATION_PROPERTY = "value.deserialization";

    public static final String VALUE_DESERIALIZATION_FOR_TOPIC_PROPERTY_PREFIX = VALUE_DESERIALIZATION_PROPERTY + ".topic.";

    private final ObjectMapper objectMapper;

    private Class valueDeserialization = Map.class;

    private Map<String, Class> valueDeserializationByTopic = Collections.emptyMap();

    public JacksonDeserializer() {
        this(JacksonHandler.initializeObjectMapper(ObjectMapper::new));
    }

    public JacksonDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.valueDeserialization = ConfigLoading.load(configs, VALUE_DESERIALIZATION_PROPERTY, TypeResolution::classForQualifiedName, valueDeserialization);
        this.valueDeserializationByTopic = ConfigLoading.loadPrefixed(configs, VALUE_DESERIALIZATION_FOR_TOPIC_PROPERTY_PREFIX, TypeResolution::classForQualifiedName);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        return JacksonHandler.convertFromString(objectMapper, new String(data), valueDeserializationByTopic.getOrDefault(topic, valueDeserialization));
    }

    @Override
    public void close() {

    }
}
