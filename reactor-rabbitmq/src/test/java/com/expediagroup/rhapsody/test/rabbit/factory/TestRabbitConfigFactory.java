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
package com.expediagroup.rhapsody.test.rabbit.factory;

import java.util.Map;

import com.expediagroup.rhapsody.rabbitmq.factory.RabbitConfigFactory;
import com.expediagroup.rhapsody.rabbitmq.serde.BodyDeserializer;
import com.expediagroup.rhapsody.rabbitmq.serde.BodySerializer;
import com.expediagroup.rhapsody.rabbitmq.serde.JacksonBodyDeserializer;
import com.expediagroup.rhapsody.rabbitmq.serde.JacksonBodySerializer;

public final class TestRabbitConfigFactory {

    private TestRabbitConfigFactory() {

    }

    public static RabbitConfigFactory createJacksonFactory(Map<String, ?> configs, Class bodyDeserialization) {
        RabbitConfigFactory rabbitConfigFactory = createFactory(configs);
        rabbitConfigFactory.put(BodySerializer.PROPERTY, JacksonBodySerializer.class.getName());
        rabbitConfigFactory.put(BodyDeserializer.PROPERTY, JacksonBodyDeserializer.class.getName());
        rabbitConfigFactory.put(JacksonBodyDeserializer.BODY_DESERIALIZATION_PROPERTY, bodyDeserialization.getName());
        return rabbitConfigFactory;
    }

    public static RabbitConfigFactory createFactory(Map<String, ?> configs) {
        RabbitConfigFactory rabbitConfigFactory = new RabbitConfigFactory();
        configs.forEach(rabbitConfigFactory::put);
        return rabbitConfigFactory;
    }
}
