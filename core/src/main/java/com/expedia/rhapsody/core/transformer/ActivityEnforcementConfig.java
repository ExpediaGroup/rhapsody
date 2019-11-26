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
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import com.expedia.rhapsody.core.factory.FieldwiseConfigFactory;
import com.expedia.rhapsody.util.ConfigLoading;

public final class ActivityEnforcementConfig {

    private final String name;            // A name that can be used to identify on what activity is being enforced

    private final Duration maxInactivity; // Max Duration before considering a stream inactive

    private final Duration delay;         // Initial delay before checking for inactivity

    private final Duration interval;      // Interval at which to check for an inactive stream

    public ActivityEnforcementConfig(Duration maxInactivity, Duration delay, Duration interval) {
        this(ActivityEnforcementConfig.class.getSimpleName(), maxInactivity, delay, interval);
    }

    public ActivityEnforcementConfig(String name, Duration maxInactivity, Duration delay, Duration interval) {
        this.name = name;
        this.maxInactivity = maxInactivity;
        this.delay = delay;
        this.interval = interval;
    }

    public boolean isEnabled() {
        return !maxInactivity.isZero() && !maxInactivity.isNegative();
    }

    public String getName() {
        return name;
    }

    public Duration getMaxInactivity() {
        return maxInactivity;
    }

    public Duration getDelay() {
        return delay;
    }

    public Duration getInterval() {
        return interval;
    }

    public static final class Factory extends FieldwiseConfigFactory<ActivityEnforcementConfig> {

        public Factory() {
            super(ActivityEnforcementConfig.class);
        }

        public static Factory disabled() {
            Factory factory = new Factory();
            factory.put("maxInactivity", "PT0S");
            return factory;
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
            Map<String, Object> defaults = new HashMap<>();
            defaults.put("delay", "PT1M");
            defaults.put("interval", "PT1M");
            return defaults;
        }

        @Override
        protected ActivityEnforcementConfig construct(Map<String, Object> configs) {
            return new ActivityEnforcementConfig(
                ConfigLoading.loadOrThrow(configs, "name", Function.identity()),
                ConfigLoading.loadOrThrow(configs, "maxInactivity", Duration::parse),
                ConfigLoading.loadOrThrow(configs, "delay", Duration::parse),
                ConfigLoading.loadOrThrow(configs, "interval", Duration::parse)
            );
        }
    }
}
