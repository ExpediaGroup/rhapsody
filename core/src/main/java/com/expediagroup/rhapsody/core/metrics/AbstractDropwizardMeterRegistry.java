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
package com.expediagroup.rhapsody.core.metrics;

import com.codahale.metrics.MetricRegistry;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.dropwizard.DropwizardConfig;
import io.micrometer.core.instrument.dropwizard.DropwizardMeterRegistry;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;

public abstract class AbstractDropwizardMeterRegistry extends DropwizardMeterRegistry {

    public AbstractDropwizardMeterRegistry(DropwizardConfig config, MetricRegistry registry, HierarchicalNameMapper nameMapper, Clock clock) {
        super(config, registry, nameMapper, clock);
        config().namingConvention(NamingConvention.identity);
    }

    @Override
    protected Double nullGaugeValue() {
        return Double.NaN;
    }
}
