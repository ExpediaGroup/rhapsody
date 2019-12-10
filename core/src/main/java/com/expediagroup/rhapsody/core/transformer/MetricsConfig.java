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
package com.expediagroup.rhapsody.core.transformer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.expediagroup.rhapsody.core.factory.FieldwiseConfigFactory;
import com.expediagroup.rhapsody.util.ConfigLoading;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

public final class MetricsConfig {

    private final String name;

    private final Iterable<Tag> tags;

    public MetricsConfig(String name, Iterable<Tag> tags) {
        this.name = name;
        this.tags = tags;
    }

    public MetricsConfig withTags(Iterable<Tag> tags) {
        return new MetricsConfig(name, Tags.of(this.tags).and(tags));
    }

    public String getName() {
        return name;
    }

    public Iterable<Tag> getTags() {
        return tags;
    }

    public static final class Factory extends FieldwiseConfigFactory<MetricsConfig> {

        public Factory() {
            super(MetricsConfig.class);
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
            return Collections.singletonMap("tags", Collections.emptyList());
        }

        @Override
        protected MetricsConfig construct(Map<String, Object> configs) {
            return new MetricsConfig(
                ConfigLoading.loadOrThrow(configs, "name", Function.identity()),
                ConfigLoading.loadCollectionOrThrow(configs, "tags", Function.identity(), Collectors.collectingAndThen(Collectors.toList(), Factory::parseTags)));
        }

        private static List<Tag> parseTags(List<String> tags) {
            return IntStream.range(0, tags.size() / 2)
                .mapToObj(i -> Tag.of(tags.get(2 * i), tags.get(2 * i + 1)))
                .collect(Collectors.toList());
        }
    }
}
