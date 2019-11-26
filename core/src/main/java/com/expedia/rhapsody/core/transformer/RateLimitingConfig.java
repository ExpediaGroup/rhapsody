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

import java.util.Collections;
import java.util.Map;

import com.expedia.rhapsody.core.factory.FieldwiseConfigFactory;
import com.expedia.rhapsody.util.ConfigLoading;

public final class RateLimitingConfig {

    private final double permitsPerSecond;

    public RateLimitingConfig(double permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
    }

    public boolean isEnabled() {
        return permitsPerSecond > 0D;
    }

    public double getPermitsPerSecond() {
        return permitsPerSecond;
    }

    public static final class Factory extends FieldwiseConfigFactory<RateLimitingConfig> {

        public Factory() {
            super(RateLimitingConfig.class);
        }

        public static Factory disabled() {
            Factory factory = new Factory();
            factory.put("permitsPerSecond", 0D);
            return factory;
        }

        @Override
        protected Map<String, Object> createDefaults() {
            return Collections.emptyMap();
        }

        @Override
        protected RateLimitingConfig construct(Map<String, Object> configs) {
            return new RateLimitingConfig(ConfigLoading.loadOrThrow(configs, "permitsPerSecond", Double::valueOf));
        }
    }
}
