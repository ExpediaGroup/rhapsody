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

import java.util.Map;

import com.expedia.rhapsody.core.factory.ConfigFactory;
import com.expedia.rhapsody.util.ConfigLoading;
import com.expedia.rhapsody.util.Defaults;

import reactor.core.scheduler.Scheduler;

public final class SchedulingConfig {

    private final SchedulerType schedulerType;

    private final Map<String, Object> schedulerConfig;

    private final boolean delayError;

    private final int prefetch;

    public SchedulingConfig(SchedulerType schedulerType, Map<String, Object> schedulerConfig, boolean delayError, int prefetch) {
        this.schedulerType = schedulerType;
        this.schedulerConfig = schedulerConfig;
        this.delayError = delayError;
        this.prefetch = prefetch;
    }

    public Scheduler createScheduler(String name) {
        return schedulerType.createScheduler(name, schedulerConfig);
    }

    public boolean isDelayError() {
        return delayError;
    }

    public int getPrefetch() {
        return prefetch;
    }

    public static final class Factory extends ConfigFactory<SchedulingConfig> {

        public Factory withSchedulerType(SchedulerType schedulerType) {
            put("schedulerType", schedulerType);
            return this;
        }

        @Override
        protected void validateProperties(Map<String, Object> properties) {
            validateEnumProperty(properties, "schedulerType", SchedulerType.class);
        }

        @Override
        protected SchedulingConfig postProcessProperties(Map<String, Object> properties) {
            return new SchedulingConfig(
                ConfigLoading.loadOrThrow(properties, "schedulerType", SchedulerType::valueOf),
                properties,
                ConfigLoading.load(properties, "delayError", Boolean::valueOf, true),
                ConfigLoading.load(properties, "prefetch", Integer::valueOf, Defaults.PREFETCH)
            );
        }

        @Override
        protected String getDefaultSpecifier() {
            return SchedulingConfig.class.getSimpleName();
        }
    }
}
