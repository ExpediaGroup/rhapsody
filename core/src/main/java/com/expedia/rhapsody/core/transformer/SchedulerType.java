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
import java.util.function.BiFunction;

import com.expedia.rhapsody.util.ConfigLoading;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public enum SchedulerType {
    IMMEDIATE((name, config) -> Schedulers.immediate()),
    SINGLE((name, config) -> Schedulers.newSingle(name, loadIsDaemon(config))),
    PARALLEL((name, config) -> Schedulers.newParallel(name, loadParallelism(config), loadIsDaemon(config))),
    ELASTIC((name, config) -> Schedulers.newElastic(name, loadTtlSeconds(config), loadIsDaemon(config)));

    private final BiFunction<String, Map<String, ?>, Scheduler> namedConfigFactory;

    SchedulerType(BiFunction<String, Map<String, ?>, Scheduler> namedConfigFactory) {
        this.namedConfigFactory = namedConfigFactory;
    }

    public Scheduler createScheduler(String name, Map<String, ?> config) {
        return namedConfigFactory.apply(name, config);
    }

    private static boolean loadIsDaemon(Map<String, ?> config) {
        return ConfigLoading.load(config, "daemon", Boolean::valueOf, false);
    }

    private static int loadParallelism(Map<String, ?> config) {
        return ConfigLoading.load(config, "parallelism", Integer::valueOf, Runtime.getRuntime().availableProcessors());
    }

    private static int loadTtlSeconds(Map<String, ?> config) {
        return ConfigLoading.load(config, "ttlSeconds", Integer::valueOf, 60);
    }
}
