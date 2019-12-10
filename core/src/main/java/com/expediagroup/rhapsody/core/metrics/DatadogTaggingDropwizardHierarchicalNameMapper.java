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

import java.util.List;
import java.util.stream.Collectors;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;

public class DatadogTaggingDropwizardHierarchicalNameMapper implements HierarchicalNameMapper {

    @Override
    public String toHierarchicalName(Meter.Id meterId, NamingConvention namingConvention) {
        return meterId.getTags().isEmpty() ?
            meterId.getConventionName(namingConvention) :
            createTaggedConventionName(meterId.getConventionName(namingConvention), meterId.getConventionTags(namingConvention));
    }

    private String createTaggedConventionName(String conventionName, List<Tag> conventionTags) {
        String tagPartial = conventionTags.stream()
            .map(tag -> tag.getKey() + "=" + tag.getValue())
            .collect(Collectors.joining(",", "(", ")"));
        return conventionName + "." + tagPartial;
    }
}
