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
package com.expediagroup.rhapsody.rabbitmq.serde;

import java.util.Map;

import com.expediagroup.rhapsody.util.JacksonHandler;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonBodySerializer<T> implements BodySerializer<T> {

    private final ObjectMapper objectMapper;

    public JacksonBodySerializer() {
        this(JacksonHandler.initializeObjectMapper(ObjectMapper::new));
    }

    public JacksonBodySerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void configure(Map<String, ?> properties) {

    }

    @Override
    public byte[] serialize(T t) {
        return JacksonHandler.convertToString(objectMapper, t).getBytes();
    }
}
