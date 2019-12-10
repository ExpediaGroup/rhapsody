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
package com.expediagroup.rhapsody.kafka.factory;

import java.util.Map;

import org.apache.kafka.clients.CommonClientConfigs;

import com.expediagroup.rhapsody.core.factory.ConfigFactory;

public final class KafkaConfigFactory extends ConfigFactory<Map<String, Object>> {

    public KafkaConfigFactory copy() {
        return copyInto(KafkaConfigFactory::new);
    }

    @Override
    public KafkaConfigFactory with(String key, Object value) {
        put(key, value);
        return this;
    }

    @Override
    protected void validateProperties(Map<String, Object> properties) {
        validateNonNullProperty(properties, CommonClientConfigs.CLIENT_ID_CONFIG);
        validateNonNullProperty(properties, CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
    }

    @Override
    protected Map<String, Object> postProcessProperties(Map<String, Object> properties) {
        return properties;
    }

    @Override
    protected String getDefaultSpecifierProperty() {
        return CommonClientConfigs.CLIENT_ID_CONFIG;
    }
}
