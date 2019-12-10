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

public final class AutoAcknowledgementConfig {

    public static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(5);

    public static final Duration DEFAULT_DELAY = Duration.ofSeconds(5);

    private final int prefetch;

    private final Duration interval;

    private final Duration delay;

    public AutoAcknowledgementConfig() {
        this(DEFAULT_INTERVAL, DEFAULT_DELAY);
    }

    public AutoAcknowledgementConfig(Duration interval, Duration delay) {
        this(Defaults.PREFETCH, interval, delay);
    }

    public AutoAcknowledgementConfig(int prefetch, Duration interval, Duration delay) {
        this.prefetch = prefetch;
        this.interval = interval;
        this.delay = delay;
    }

    public int getPrefetch() {
        return prefetch;
    }

    public Duration getInterval() {
        return interval;
    }

    public Duration getDelay() {
        return delay;
    }

    public static final class Factory extends FieldwiseConfigFactory<AutoAcknowledgementConfig> {

        public Factory() {
            super(AutoAcknowledgementConfig.class);
        }

        @Override
        protected Map<String, Object> createDefaults() {
            Map<String, Object> defaults = new HashMap<>();
            defaults.put("prefetch", Defaults.PREFETCH);
            defaults.put("interval", DEFAULT_INTERVAL);
            defaults.put("delay", DEFAULT_DELAY);
            return defaults;
        }

        @Override
        protected AutoAcknowledgementConfig construct(Map<String, Object> configs) {
            return new AutoAcknowledgementConfig(
                ConfigLoading.loadOrThrow(configs, "prefetch", Integer::valueOf),
                ConfigLoading.loadOrThrow(configs, "interval", Duration::parse),
                ConfigLoading.loadOrThrow(configs, "delay", Duration::parse)
            );
        }
    }
}
