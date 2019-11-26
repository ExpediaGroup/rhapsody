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
package com.expedia.rhapsody.core.metrics;

import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

public final class MeterKey {

    private final String name;

    private final Tags tags;

    public MeterKey(String name, Map<String, String> tags) {
        this(name, tags.entrySet().stream().map(entry -> Tag.of(entry.getKey(), entry.getValue())).collect(Collectors.toList()));
    }

    public MeterKey(String name, Iterable<Tag> tags) {
        this(name, Tags.of(tags));
    }

    public MeterKey(String name, Tags tags) {
        this.name = name;
        this.tags = tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MeterKey meterKey = (MeterKey) o;
        return Objects.equals(name, meterKey.name) &&
            Objects.equals(tags, meterKey.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, tags);
    }

    @Override
    public String toString() {
        return "MeterKey{name='" + name + "', tags=" + tags + "}";
    }

    public String getName() {
        return name;
    }

    public Iterable<Tag> getTags() {
        return tags;
    }
}
