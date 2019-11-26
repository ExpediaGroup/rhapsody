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
package com.expedia.rhapsody.kafka.metrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MicrometerMetricsReporterTest {

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    private final MicrometerMetricsReporter reporter = new TestMicrometerMetricsReporter(meterRegistry);

    @Before
    public void setup() {
        reporter.configure(Collections.emptyMap());
    }

    @After
    public void teardown() {
        reporter.close();
    }

    @Test
    public void metricsAreReportedViaMicrometer() {
        Tag tag = Tag.of("key", "value");
        double value = Math.random();
        KafkaMetric metric = createMetric("group", "name", Collections.singletonMap(tag.getKey(), tag.getValue()), () -> value);

        reporter.init(Collections.singletonList(metric));

        Gauge gauge = meterRegistry.find("kafka.group.name").tags(Collections.singletonList(tag)).gauge();

        assertNotNull(gauge);
        assertEquals(value, gauge.value(), 0D);
    }

    @Test
    public void removedMetricsReportNaN() {
        KafkaMetric metric = createMetric("group", "name", Collections.emptyMap(), Math::random);

        reporter.metricChange(metric);
        reporter.metricRemoval(metric);

        Gauge gauge = meterRegistry.find("kafka.group.name").gauge();

        assertNotNull(gauge);
        assertTrue(Double.isNaN(gauge.value()));
    }

    @Test
    public void metricsOnClosedReporterReportNaN() {
        KafkaMetric metric = createMetric("group", "name", Collections.emptyMap(), Math::random);

        reporter.metricChange(metric);
        reporter.close();

        Gauge gauge = meterRegistry.find("kafka.group.name").gauge();

        assertNotNull(gauge);
        assertTrue(Double.isNaN(gauge.value()));
    }

    @Test
    public void reRegisteredMetricsReportCorrectly() {
        double value = Math.random();
        KafkaMetric metric = createMetric("group", "name", Collections.emptyMap(), () -> value);

        reporter.metricChange(metric);
        reporter.metricRemoval(metric);
        reporter.metricChange(metric);

        Gauge gauge = meterRegistry.find("kafka.group.name").gauge();

        assertNotNull(gauge);
        assertEquals(value, gauge.value(), 0D);
    }

    @Test
    public void reRegisteredMetricsOnNewReporterReportCorrectly() {
        double value = Math.random();
        KafkaMetric metric = createMetric("group", "name", Collections.emptyMap(), () -> value);

        reporter.metricChange(metric);
        reporter.close();

        MicrometerMetricsReporter newReporter = new TestMicrometerMetricsReporter(meterRegistry);
        newReporter.metricChange(metric);

        Gauge gauge = meterRegistry.find("kafka.group.name").gauge();

        assertNotNull(gauge);
        assertEquals(value, gauge.value(), 0D);
    }

    @Test
    public void blacklistedMetricsAreNotReported() {
        double value = Math.random();
        KafkaMetric reportedMetric = createMetric("group", "reported", Collections.emptyMap(), () -> value);
        KafkaMetric unreportedMetric = createMetric("group", "unreported", Collections.emptyMap(), Math::random);

        Map<String, String> config = new HashMap<>();
        config.put(MicrometerMetricsReporter.FILTER_NAMES_INCLUSION_CONFIG, MicrometerMetricsReporter.FilterInclusion.BLACKLIST.name());
        config.put(MicrometerMetricsReporter.FILTER_NAMES_CONFIG, "unreported");
        reporter.configure(config);
        reporter.init(Arrays.asList(reportedMetric, unreportedMetric));

        assertNotNull(meterRegistry.find("kafka.group.reported").gauge());
        assertNull(meterRegistry.find("kafka.group.unreported").gauge());
    }

    @Test
    public void onlyWhitelistedMetricsAreReported() {
        double value = Math.random();
        KafkaMetric reportedMetric = createMetric("group", "reported", Collections.emptyMap(), () -> value);
        KafkaMetric unreportedMetric = createMetric("group", "unreported", Collections.emptyMap(), Math::random);

        Map<String, String> config = new HashMap<>();
        config.put(MicrometerMetricsReporter.FILTER_NAMES_INCLUSION_CONFIG, MicrometerMetricsReporter.FilterInclusion.WHITELIST.name());
        config.put(MicrometerMetricsReporter.FILTER_NAMES_CONFIG, "reported");
        reporter.configure(config);
        reporter.init(Arrays.asList(reportedMetric, unreportedMetric));

        assertNotNull(meterRegistry.find("kafka.group.reported").gauge());
        assertNull(meterRegistry.find("kafka.group.unreported").gauge());
    }

    private KafkaMetric createMetric(String group, String name, Map<String, String> tags, Supplier<Double> metricValueSupplier) {
        MetricName metricName = new MetricName(name, group, "TEST", tags);
        Measurable measurable = (config, now) -> metricValueSupplier.get();
        return new KafkaMetric(this, metricName, measurable, new MetricConfig(), Time.SYSTEM);
    }

    private static final class TestMicrometerMetricsReporter extends MicrometerMetricsReporter {

        private final MeterRegistry meterRegistry;

        private TestMicrometerMetricsReporter(MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;
        }

        @Override
        protected MeterRegistry createMeterRegistry(Map<String, ?> configs) {
            return meterRegistry;
        }
    }
}