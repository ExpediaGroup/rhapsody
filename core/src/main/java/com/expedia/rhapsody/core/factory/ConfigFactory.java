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
package com.expedia.rhapsody.core.factory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.expedia.rhapsody.util.ConfigLoading;
import com.expedia.rhapsody.util.Instantiation;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

public abstract class ConfigFactory<T> {

    public static final String DEFAULT_SPECIFIER_PROPERTY = "specifier";

    public static final String PROVIDERS_PROPERTY = "providers";

    public static final String ENVIRONMENTAL_OVERRIDE_PREFIX = "config.override.";

    public static final String RANDOMIZE_PROPERTY_SUFFIX = ".randomize";

    private Map<String, Object> properties = new HashMap<>();

    public final T create() {
        String specifier = Optional.ofNullable(get(getDefaultSpecifierProperty()))
            .map(Object::toString)
            .orElseGet(this::getDefaultSpecifier);
        return create(specifier);
    }

    public final T create(String specifier) {
        Map<String, Object> preProcessedProperties = preProcessProperties(specifier, Collections.unmodifiableMap(properties));
        Map<String, Object> processedProperties = processProperties(specifier, preProcessedProperties);
        return postProcessProperties(processedProperties);
    }

    public ConfigFactory<T> with(String key, Object value) {
        put(key, value);
        return this;
    }

    @JsonAnyGetter
    public Object get(String key) {
        return properties.get(key);
    }

    @JsonAnySetter
    public Object put(String key, Object value) {
        return properties.put(key, value);
    }

    protected <R extends ConfigFactory> R copyInto(Supplier<R> copySupplier) {
        R copy = copySupplier.get();
        properties.forEach(copy::put);
        return copy;
    }

    protected Map<String, Object> preProcessProperties(String specifier, Map<String, Object> properties) {
        Map<String, Object> providedProperties = applyProviders(specifier, properties);
        validateProperties(providedProperties);
        return providedProperties;
    }

    protected Map<String, Object> applyProviders(String specifier, Map<String, Object> properties) {
        return ConfigLoading.loadCollection(properties, PROVIDERS_PROPERTY, providerClass -> instantiateProvider(providerClass, properties),
            Collectors.reducing(properties, propertyProvider -> propertyProvider.provide(specifier), ConfigFactory::mergeConfigs)).orElse(properties);
    }

    protected Map<String, Object> processProperties(String specifier, Map<String, Object> properties) {
        return conditionallyRandomize(applyOverrides(specifier, properties));
    }

    protected Map<String, Object> applyOverrides(String specifier, Map<String, Object> properties) {
        Map<String, Object> result = new HashMap<>(properties);
        ConfigLoading.loadPrefixedEnvironmentalProperties(ENVIRONMENTAL_OVERRIDE_PREFIX)
            .forEach((key, value) -> result.computeIfPresent(key, (k, v) -> value));
        ConfigLoading.loadPrefixedEnvironmentalProperties(ENVIRONMENTAL_OVERRIDE_PREFIX + specifier + ".")
            .forEach(result::put);
        return result;
    }

    protected abstract void validateProperties(Map<String, Object> properties);

    protected abstract T postProcessProperties(Map<String, Object> properties);

    protected String getDefaultSpecifierProperty() {
        return DEFAULT_SPECIFIER_PROPERTY;
    }

    protected String getDefaultSpecifier() {
        throw new UnsupportedOperationException("This ConfigFactory does not have a default specifier");
    }

    protected static void validateNonNullProperty(Map<String, Object> properties, String key) {
        Objects.requireNonNull(properties.get(key), key + " is a required Configuration");
    }

    protected static <T extends Enum<T>> void validateEnumProperty(Map<String, Object> properties, String key, Class<T> enumClass) {
        try {
            Enum.valueOf(enumClass, Objects.toString(properties.get(key)));
        } catch (Exception e) {
            throw new IllegalArgumentException(key + " must be configured as an Enum value from " + enumClass, e);
        }
    }

    private static PropertyProvider instantiateProvider(String providerClass, Map<String, Object> configs) {
        PropertyProvider propertyProvider = Instantiation.one(providerClass);
        propertyProvider.configure(configs);
        return propertyProvider;
    }

    private static Map<String, Object> mergeConfigs(Map<String, ?> configs1, Map<String, ?> configs2) {
        Map<String, Object> merged = new HashMap<>(configs1);
        merged.putAll(configs2);
        return merged;
    }

    private static Map<String, Object> conditionallyRandomize(Map<String, Object> configs) {
        Map<String, Object> randomizationConfig = configs.entrySet().stream()
            .filter(entry -> entry.getKey().endsWith(RANDOMIZE_PROPERTY_SUFFIX))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return configs.entrySet().stream()
            .filter(entry -> !randomizationConfig.containsKey(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey,
                entry -> shouldRandomize(randomizationConfig, entry.getKey()) ? randomize(entry.getValue()) : entry.getValue()));
    }

    private static boolean shouldRandomize(Map<String, Object> randomizationConfig, String property) {
        return Objects.equals(Boolean.toString(true), Objects.toString(randomizationConfig.get(property + RANDOMIZE_PROPERTY_SUFFIX)));
    }

    private static String randomize(Object value) {
        return value + "-" + UUID.randomUUID();
    }
}
