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
import java.util.HashMap;
import java.util.Map;

import com.expediagroup.rhapsody.core.factory.FieldwiseConfigFactory;
import com.expediagroup.rhapsody.util.ConfigLoading;
import com.expediagroup.rhapsody.util.Defaults;

public final class WorkBufferConfig {

    private final Duration bufferDuration;

    private final long maxBufferSize;

    private final int bufferConcurrency;

    public WorkBufferConfig(Duration bufferDuration, long maxBufferSize, int bufferConcurrency) {
        this.bufferDuration = bufferDuration;
        this.maxBufferSize = maxBufferSize;
        this.bufferConcurrency = bufferConcurrency;
    }

    public Duration getBufferDuration() {
        return bufferDuration;
    }

    public long getMaxBufferSize() {
        return maxBufferSize;
    }

    public int getBufferConcurrency() {
        return bufferConcurrency;
    }

    public static final class Factory extends FieldwiseConfigFactory<WorkBufferConfig> {

        public Factory() {
            super(WorkBufferConfig.class);
        }

        @Override
        protected Map<String, Object> createDefaults() {
            Map<String, Object> defaults = new HashMap<>();
            defaults.put("bufferDuration", "PT10S");
            defaults.put("maxBufferSize", 8);
            defaults.put("bufferConcurrency", Defaults.CONCURRENCY);
            return defaults;
        }

        @Override
        protected WorkBufferConfig construct(Map<String, Object> configs) {
            return new WorkBufferConfig(
                ConfigLoading.loadOrThrow(configs, "bufferDuration", Duration::parse),
                ConfigLoading.loadOrThrow(configs, "maxBufferSize", Long::valueOf),
                ConfigLoading.loadOrThrow(configs, "bufferConcurrency", Integer::valueOf)
            );
        }
    }
}
