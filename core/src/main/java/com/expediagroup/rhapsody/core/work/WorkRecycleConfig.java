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
package com.expediagroup.rhapsody.core.work;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.expediagroup.rhapsody.core.factory.FieldwiseConfigFactory;
import com.expediagroup.rhapsody.util.ConfigLoading;
import com.expediagroup.rhapsody.util.TypeResolution;

public final class WorkRecycleConfig {

    private final Duration recycleExpiration;

    private final long maxRecycleCount;

    private final Set<Class> unrecyclableErrors;

    public WorkRecycleConfig(Duration recycleExpiration, long maxRecycleCount, Set<Class> unrecyclableErrors) {
        this.recycleExpiration = recycleExpiration;
        this.maxRecycleCount = maxRecycleCount;
        this.unrecyclableErrors = unrecyclableErrors;
    }

    public Duration getRecycleExpiration() {
        return recycleExpiration;
    }

    public long getMaxRecycleCount() {
        return maxRecycleCount;
    }
    
    public Set<Class> getUnrecyclableErrors() {
        return unrecyclableErrors;
    }

    public static final class Factory extends FieldwiseConfigFactory<WorkRecycleConfig> {

        public Factory() {
            super(WorkRecycleConfig.class);
        }

        public Factory withRecycleExpiration(Duration recycleExpiration) {
            put("recycleExpiration", recycleExpiration);
            return this;
        }

        @Override
        protected Map<String, Object> createDefaults() {
            Map<String, Object> defaults = new HashMap<>();
            defaults.put("recycleExpiration", "P2D");
            defaults.put("maxRecycleCount", Long.MAX_VALUE);
            defaults.put("unrecyclableErrors", Collections.emptyList());
            return defaults;
        }

        @Override
        protected WorkRecycleConfig construct(Map<String, Object> configs) {
            return new WorkRecycleConfig(
                ConfigLoading.loadOrThrow(configs, "recycleExpiration", Duration::parse),
                ConfigLoading.loadOrThrow(configs, "maxRecycleCount", Long::valueOf),
                ConfigLoading.loadCollectionOrThrow(configs, "unrecyclableErrors", TypeResolution::classForQualifiedName, Collectors.toSet())
            );
        }
    }
}
