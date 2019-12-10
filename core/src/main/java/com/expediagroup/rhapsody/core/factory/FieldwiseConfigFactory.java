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
package com.expediagroup.rhapsody.core.factory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.expediagroup.rhapsody.util.FieldResolution;

public abstract class FieldwiseConfigFactory<T> extends ConfigFactory<T> {

    private static final Map<Class, Collection<Field>> FIELDS_BY_CONFIG = new ConcurrentHashMap<>();

    private final Class<T> configClass;

    private final Map<String, Object> defaults;

    public FieldwiseConfigFactory(Class<T> configClass) {
        this.configClass = configClass;
        this.defaults = createDefaults();
    }

    protected abstract Map<String, Object> createDefaults();

    @Override
    protected void validateProperties(Map<String, Object> properties) {
        Set<String> missingConfigFieldNames = getFieldsForConfig(configClass).stream()
            .map(Field::getName)
            .filter(fieldName -> !defaults.containsKey(fieldName) && !properties.containsKey(fieldName))
            .collect(Collectors.toSet());
        if (!missingConfigFieldNames.isEmpty()) {
            throw new IllegalArgumentException("Config Fields are missing: " + missingConfigFieldNames.stream().collect(Collectors.joining(",")));
        }
    }

    @Override
    protected T postProcessProperties(Map<String, Object> properties) {
        Map<String, Object> configs = new HashMap<>(defaults);
        configs.putAll(properties);
        return construct(configs);
    }

    protected abstract T construct(Map<String, Object> configs);

    @Override
    protected String getDefaultSpecifier() {
        return configClass.getSimpleName();
    }

    private static Collection<Field> getFieldsForConfig(Class configClass) {
        return FIELDS_BY_CONFIG.computeIfAbsent(configClass, FieldwiseConfigFactory::extractFieldsForConfig);
    }

    private static Collection<Field> extractFieldsForConfig(Class configClass) {
        return FieldResolution.getAllFields(configClass).stream()
            .filter(field -> !Modifier.isStatic(field.getModifiers()) && !Modifier.isTransient(field.getModifiers()))
            .collect(Collectors.toSet());
    }
}
