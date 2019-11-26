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
package com.expedia.rhapsody.core.transformer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.expedia.rhapsody.core.factory.FieldwiseConfigFactory;
import com.expedia.rhapsody.util.ConfigLoading;

public final class ResubscriptionConfig {

    private final String name;

    private final Duration delay;

    public ResubscriptionConfig(Duration delay) {
        this(ResubscriptionConfig.class.getSimpleName(), delay);
    }

    public ResubscriptionConfig(String name, Duration delay) {
        this.name = name;
        this.delay = delay;
    }

    public boolean isEnabled() {
        return !delay.isNegative();
    }

    public String getName() {
        return name;
    }

    public Duration getDelay() {
        return delay;
    }

    public static final class Factory extends FieldwiseConfigFactory<ResubscriptionConfig> {

        public Factory() {
            super(ResubscriptionConfig.class);
        }

        @Override
        protected Map<String, Object> preProcessProperties(String specifier, Map<String, Object> properties) {
            Map<String, Object> preProcessedProperties = new HashMap<>(properties);
            preProcessedProperties.putIfAbsent("name", specifier);
            return super.preProcessProperties(specifier, preProcessedProperties);
        }

        @Override
        protected String getDefaultSpecifierProperty() {
            return "name";
        }

        @Override
        protected Map<String, Object> createDefaults() {
            return Collections.singletonMap("delay", "PT20S");
        }

        @Override
        protected ResubscriptionConfig construct(Map<String, Object> configs) {
            return new ResubscriptionConfig(
                ConfigLoading.loadOrThrow(configs, "name", Function.identity()),
                ConfigLoading.loadOrThrow(configs, "delay", Duration::parse)
            );
        }
    }
}
