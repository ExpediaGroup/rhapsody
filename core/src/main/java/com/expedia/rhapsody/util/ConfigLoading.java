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
package com.expedia.rhapsody.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ConfigLoading {

    private ConfigLoading() {

    }

    public static <T> T load(Map<String, ?> configs, String property, Function<? super String, T> parser, T defaultValue) {
        return load(configs, property, parser).orElse(defaultValue);
    }

    public static <T> T loadOrThrow(Map<String, ?> configs, String property, Function<? super String, T> parser) {
        return load(configs, property, parser).orElseThrow(supplyMissingConfigPropertyException(property));
    }

    public static <T> Optional<T> load(Map<String, ?> configs, String property, Function<? super String, T> parser) {
        return Optional.ofNullable(configs.get(property)).map(Object::toString).map(parser);
    }

    public static <T, R> R loadCollectionOrThrow(Map<String, ?> configs, String property, Function<? super String, T> parser, Collector<? super T, ?, R> collector) {
        return loadCollection(configs, property, parser, collector).orElseThrow(supplyMissingConfigPropertyException(property));
    }

    public static <T, R> Optional<R> loadCollection(Map<String, ?> configs, String property, Function<? super String, T> parser, Collector<? super T, ?, R> collector) {
        return Optional.ofNullable(configs.get(property))
            .map(ConfigLoading::convertToCollection)
            .map(collection -> collection.stream().map(Objects::toString).map(parser).collect(collector));
    }

    public static Map<String, Object> loadPrefixedEnvironmentalProperties(String prefix) {
        return Stream.of(System.getenv().entrySet(), System.getProperties().entrySet())
            .flatMap(Collection::stream)
            .filter(entry -> Objects.toString(entry.getKey()).startsWith(prefix))
            .collect(Collectors.toMap(entry -> entry.getKey().toString().substring(prefix.length()), Map.Entry::getValue,
                (firstValue, secondValue) -> secondValue));
    }

    public static <T> Map<String, T> loadPrefixed(Map<String, ?> configs, String prefix, Function<? super String, T> parser) {
        return configs.entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(prefix))
            .collect(Collectors.toMap(entry -> entry.getKey().substring(prefix.length()), entry -> parser.apply(entry.getValue().toString())));
    }

    private static Collection<?> convertToCollection(Object config) {
        if (config instanceof Collection) {
            return Collection.class.cast(config);
        } else if (config instanceof CharSequence) {
            return Arrays.asList(config.toString().split(","));
        } else {
            return Collections.singletonList(config);
        }
    }

    private static Supplier<RuntimeException> supplyMissingConfigPropertyException(String property) {
        return () -> new IllegalArgumentException("Missing config: " + property);
    }
}
