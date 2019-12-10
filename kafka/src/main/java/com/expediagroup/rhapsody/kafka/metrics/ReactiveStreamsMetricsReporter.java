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
package com.expediagroup.rhapsody.kafka.metrics;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.metrics.KafkaMetric;

public class ReactiveStreamsMetricsReporter extends MicrometerMetricsReporter {

    /**
     * Some Metric names are redundantly qualified/prefixed with String representations of their
     * tags (See Fetcher::FetchManagerMetrics). We strip these off and rely on using said tags to
     * differentiate Metrics from one-another.
     */
    @Override
    protected String extractMetricName(KafkaMetric metric) {
        return removeUpToLastAndIncluding(metric.metricName().name(), '.');
    }

    @Override
    protected Map<String, String> extractTags(KafkaMetric metric) {
        Map<String, String> tags = new HashMap<>(super.extractTags(metric));
        tags.computeIfPresent(CLIENT_ID_TAG, (key, value) -> sanitizeClientId(value));
        return tags;
    }

    private String sanitizeClientId(String clientId) {
        return clientId.replaceAll("-\\d+$", "");
    }
}
