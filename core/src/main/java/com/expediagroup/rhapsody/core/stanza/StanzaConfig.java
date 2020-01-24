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
package com.expediagroup.rhapsody.core.stanza;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import com.expediagroup.rhapsody.api.StreamListener;
import com.expediagroup.rhapsody.core.transformer.ActivityEnforcementConfig;
import com.expediagroup.rhapsody.core.transformer.MetricsConfig;
import com.expediagroup.rhapsody.core.transformer.RateLimitingConfig;
import com.expediagroup.rhapsody.core.transformer.ResubscriptionConfig;
import com.expediagroup.rhapsody.core.transformer.SchedulerType;
import com.expediagroup.rhapsody.core.transformer.SchedulingConfig;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;

public interface StanzaConfig {

    default Optional<MeterRegistry> meterRegistry() {
        return Optional.empty();
    }

    default Collection<StreamListener> streamListeners() {
        return Collections.emptyList();
    }

    default ActivityEnforcementConfig activityEnforcement() {
        return ActivityEnforcementConfig.Factory.disabled().create(name());
    }

    default ResubscriptionConfig resubscription() {
        return ResubscriptionConfig.Factory.disabled().create(name());
    }

    default RateLimitingConfig rateLimiting() {
        return RateLimitingConfig.Factory.disabled().create(name());
    }

    default MetricsConfig metrics() {
        return new MetricsConfig(getClass().getName(), Tags.of("name", name()));
    }

    default SchedulingConfig scheduling() {
        return new SchedulingConfig.Factory().withSchedulerType(SchedulerType.IMMEDIATE).create(name());
    }

    String name();
}
