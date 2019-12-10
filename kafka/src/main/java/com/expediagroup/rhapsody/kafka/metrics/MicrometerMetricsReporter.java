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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.rhapsody.core.metrics.AuditedMetricRegistry;
import com.expediagroup.rhapsody.core.metrics.MeterKey;
import com.expediagroup.rhapsody.util.ConfigLoading;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;

public abstract class MicrometerMetricsReporter implements MetricsReporter {

    public enum FilterInclusion { BLACKLIST, WHITELIST }

    public static final String FILTER_NAMES_INCLUSION_CONFIG = "metric.filter.names.inclusion";

    public static final String FILTER_NAMES_CONFIG = "metric.filter.names";

    protected static final String CLIENT_ID_TAG = "client-id";

    private static final Logger LOGGER = LoggerFactory.getLogger(MicrometerMetricsReporter.class);

    private static final double ABSENT_EVALUATION = Double.NaN;

    private static final AuditedMetricRegistry<MicrometerMetricsReporter, KafkaMetric, Double> AUDITED_METRIC_REGISTRY =
        new AuditedMetricRegistry<>(MicrometerMetricsReporter::extractMeterValue, ABSENT_EVALUATION);

    protected MeterRegistry meterRegistry = Metrics.globalRegistry;

    private FilterInclusion filteredMetricNamesInclusion = FilterInclusion.BLACKLIST;

    private Collection<String> filteredMetricNames = Collections.emptySet();

    @Override
    public void configure(Map<String, ?> configs) {
        this.meterRegistry = createMeterRegistry(configs);
        this.filteredMetricNamesInclusion = ConfigLoading.load(configs, FILTER_NAMES_INCLUSION_CONFIG, FilterInclusion::valueOf, filteredMetricNamesInclusion);
        this.filteredMetricNames = ConfigLoading.loadCollection(configs, FILTER_NAMES_CONFIG, Function.identity(), Collectors.toSet()).orElseGet(Collections::emptySet);
    }

    @Override
    public final void init(List<KafkaMetric> metrics) {
        metrics.forEach(this::metricChange);
    }

    @Override
    public final void metricChange(KafkaMetric metric) {
        String extractedMetricName = extractMetricName(metric);
        if (shouldReportMetric(metric, extractedMetricName)) {
            registerMetric(createMeterKey(metric, extractedMetricName), metric);
        }
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        AUDITED_METRIC_REGISTRY.unregister(createMeterKey(metric, extractMetricName(metric)), this);
    }

    @Override
    public void close() {
        AUDITED_METRIC_REGISTRY.unregister(this);
    }

    protected MeterRegistry createMeterRegistry(Map<String, ?> configs) {
        return Metrics.globalRegistry;
    }

    protected String extractMetricName(KafkaMetric metric) {
        return metric.metricName().name();
    }

    protected boolean shouldReportMetric(KafkaMetric metric, String extractedMetricName) {
        return FilterInclusion.WHITELIST.equals(filteredMetricNamesInclusion) == filteredMetricNames.contains(extractedMetricName);
    }

    protected final MeterKey createMeterKey(KafkaMetric metric, String extractedMetricName) {
        return new MeterKey(extractMeterNamePrefix(metric) + extractedMetricName, extractTags(metric));
    }

    protected String extractMeterNamePrefix(KafkaMetric metric) {
        return "kafka." + metric.metricName().group() + ".";
    }

    protected Map<String, String> extractTags(KafkaMetric metric) {
        return metric.metricName().tags();
    }

    protected final void registerMetric(MeterKey meterKey, KafkaMetric metric) {
        // Note that the MeterKey used to register a Gauge is the one returned by registration
        // with the AuditedMetricRegistry, which keeps an interned Strong Reference to all past
        // and current registered MeterKeys in order to prevent removal of said MeterKeys by
        // garbage collection, which is possible due to Weak Referencing of MeterKeys by underlying
        // MeterRegistry implementations
        MeterKey registeredMeterKey = AUDITED_METRIC_REGISTRY.register(meterKey, this, metric);
        if (meterRegistry.find(registeredMeterKey.getName()).tags(registeredMeterKey.getTags()).meter() == null) {
            registerGauge(meterRegistry, registeredMeterKey, metric);
        }
    }

    protected final double extractMeterValue(KafkaMetric metric) {
        return Optional.ofNullable(metric.metricValue())
            .filter(Number.class::isInstance)
            .map(Number.class::cast)
            .flatMap(this::extractMeterValue)
            .orElse(ABSENT_EVALUATION);
    }

    protected Optional<Double> extractMeterValue(Number number) {
        return Optional.of(number.doubleValue());
    }

    protected static String removeUuids(String string) {
        return string.replaceAll("-?[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}", "");
    }

    protected static String removeUpToLastAndIncluding(String string, char toRemove) {
        return string.substring(string.lastIndexOf(toRemove) + 1);
    }

    private static void registerGauge(MeterRegistry meterRegistry, MeterKey meterKey, KafkaMetric metric) {
        try {
            // Note that depending on the MeterRegistry implementation, the registered MeterKey may
            // only be Weakly Referenced, which means it is susceptible garbage collection if not
            // Strongly Referenced elsewhere (i.e. by an AuditedMetricRegistry)
            Gauge.builder(meterKey.getName(), meterKey, AUDITED_METRIC_REGISTRY::evaluate)
                .description(metric.metricName().description())
                .tags(meterKey.getTags())
                .register(meterRegistry);
        } catch (Exception e) {
            LOGGER.debug("Failed to register Gauge with key={}", meterKey, e);
        }
    }
}
