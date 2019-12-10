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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;

/**
 * This isn't actually intended to be used, but rather is a result of investigation in to how
 * Kafka metrics were collected prior to usage of ReactiveStreams.
 */
@Deprecated
public class KafkaStreamsMetricsReporter extends MicrometerMetricsReporter {

    protected static final String PROCESSOR_NODE_ID_TAG = "processor-node-id";

    private static final Pattern STREAM_THREAD_PATTERN = Pattern.compile("^(?:(.+?)-)??StreamThread-(\\d+)(?:-(.+))??$");

    @Override
    protected String extractMetricName(KafkaMetric metric) {
        return removeUpToLastAndIncluding(metric.metricName().name(), '.');
    }

    @Override
    protected Map<String, String> extractTags(KafkaMetric metric) {
        Map<String, String> tags = new HashMap<>();
        tags.putAll(extractProcessTags(metric.metricName()));
        tags.putAll(extractClientTags(metric.metricName()));
        Optional.ofNullable(metric.metricName().tags().get("topic"))
            .ifPresent(topic -> tags.put("topic", topic));
        return tags;
    }

    protected Map<String, String> extractProcessTags(MetricName metricName) {
        if (metricName.tags().containsKey(PROCESSOR_NODE_ID_TAG)) {
            return Collections.singletonMap(PROCESSOR_NODE_ID_TAG, metricName.tags().get(PROCESSOR_NODE_ID_TAG));
        } else if (metricName.name().contains(".")) {
            return Collections.singletonMap("global-thread", metricName.name().substring(0, metricName.name().lastIndexOf('.')));
        } else {
            return Collections.emptyMap();
        }
    }

    protected Map<String, String> extractClientTags(MetricName metricName) {
        return Optional.ofNullable(metricName.tags().get(CLIENT_ID_TAG))
            .map(this::extractClientIdTags)
            .orElseGet(Collections::emptyMap);
    }

    protected Map<String, String> extractClientIdTags(String clientId) {
        Matcher streamThreadMatcher = STREAM_THREAD_PATTERN.matcher(clientId);
        return streamThreadMatcher.matches() ? extractStreamThreadTags(streamThreadMatcher) : Collections.singletonMap(CLIENT_ID_TAG, removeUuids(clientId));
    }

    protected Map<String, String> extractStreamThreadTags(Matcher clientIdStreamThreadMatcher) {
        Map<String, String> streamThreadTags = new HashMap<>();
        streamThreadTags.put(CLIENT_ID_TAG, removeUuids(extractClientId(clientIdStreamThreadMatcher)));
        Optional.ofNullable(clientIdStreamThreadMatcher.group(2))
            .ifPresent(threadNumber -> streamThreadTags.put("stream-thread", threadNumber));
        Optional.ofNullable(clientIdStreamThreadMatcher.group(3))
            .ifPresent(clientType -> streamThreadTags.put("client-type", clientType));
        return streamThreadTags;
    }

    @Override
    protected Optional<Double> extractMeterValue(Number number) {
        return Optional.of(number.doubleValue())
            .map(value -> Double.isFinite(value) ? value : 0D);
    }

    private static String extractClientId(Matcher clientIdStreamThreadMatcher) {
        return Optional.ofNullable(clientIdStreamThreadMatcher.group(1))
            .orElse(clientIdStreamThreadMatcher.group());
    }
}
