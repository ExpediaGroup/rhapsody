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
package com.expediagroup.rhapsody.core.metrics;

import java.util.function.Function;

import io.micrometer.core.instrument.dropwizard.DropwizardConfig;

public final class ComposedDropwizardMeterRegistryConfig implements DropwizardConfig {

    private final String prefix;

    private final Function<String, String> function;

    public ComposedDropwizardMeterRegistryConfig() {
        this("", key -> null);
    }

    public ComposedDropwizardMeterRegistryConfig(String prefix) {
        this(prefix, key -> null);
    }

    public ComposedDropwizardMeterRegistryConfig(Function<String, String> function) {
        this("", function);
    }

    public ComposedDropwizardMeterRegistryConfig(String prefix, Function<String, String> function) {
        this.prefix = prefix;
        this.function = function;
    }

    @Override
    public String prefix() {
        return prefix;
    }

    @Override
    public String get(String key) {
        return function.apply(key);
    }
}
