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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import com.expediagroup.rhapsody.core.factory.FieldwiseConfigFactory;
import com.expediagroup.rhapsody.util.ConfigLoading;
import com.expediagroup.rhapsody.util.Defaults;

public final class DeduplicationConfig {

    private final int deduplicationSourcePrefetch;

    private final Duration deduplicationDuration;

    private final long maxDeduplicationSize;

    private final int deduplicationConcurrency;

    @Deprecated
    public DeduplicationConfig(Duration deduplicationDuration, long maxDeduplicationSize, int deduplicationConcurrency) {
        this(Defaults.PREFETCH, deduplicationDuration, maxDeduplicationSize, deduplicationConcurrency);
    }

    public DeduplicationConfig(int deduplicationSourcePrefetch, Duration deduplicationDuration, long maxDeduplicationSize, int deduplicationConcurrency) {
        this.deduplicationSourcePrefetch = deduplicationSourcePrefetch;
        this.deduplicationDuration = deduplicationDuration;
        this.maxDeduplicationSize = maxDeduplicationSize;
        this.deduplicationConcurrency = deduplicationConcurrency;
    }

    public int getDeduplicationSourcePrefetch() {
        return deduplicationSourcePrefetch;
    }

    public boolean isEnabled() {
        return !deduplicationDuration.isNegative() && !deduplicationDuration.isZero();
    }

    public Duration getDeduplicationDuration() {
        return deduplicationDuration;
    }

    public long getMaxDeduplicationSize() {
        return maxDeduplicationSize;
    }

    public int getDeduplicationConcurrency() {
        return deduplicationConcurrency;
    }

    public static final class Factory extends FieldwiseConfigFactory<DeduplicationConfig> {

        public Factory() {
            super(DeduplicationConfig.class);
        }

        public static Factory disabled() {
            Factory factory = new Factory();
            factory.put("deduplicationDuration", "PT0S");
            return factory;
        }

        @Override
        protected Map<String, Object> createDefaults() {
            Map<String, Object> defaults = new HashMap<>();
            defaults.put("deduplicationSourcePrefetch", Defaults.PREFETCH);
            defaults.put("deduplicationDuration", "PT1S");
            defaults.put("maxDeduplicationSize", Long.MAX_VALUE);
            defaults.put("deduplicationConcurrency", Defaults.CONCURRENCY);
            return defaults;
        }

        @Override
        protected DeduplicationConfig construct(Map<String, Object> configs) {
            return new DeduplicationConfig(
                ConfigLoading.loadOrThrow(configs, "deduplicationSourcePrefetch", Integer::valueOf),
                ConfigLoading.loadOrThrow(configs, "deduplicationDuration", Duration::parse),
                ConfigLoading.loadOrThrow(configs, "maxDeduplicationSize", Long::valueOf),
                ConfigLoading.loadOrThrow(configs, "deduplicationConcurrency", Integer::valueOf)
            );
        }
    }
}
