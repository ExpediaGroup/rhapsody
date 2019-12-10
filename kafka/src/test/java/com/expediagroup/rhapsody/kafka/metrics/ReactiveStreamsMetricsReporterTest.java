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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import static org.junit.Assert.assertNotNull;

public class ReactiveStreamsMetricsReporterTest {

    private final MeterRegistry meterRegistry = new SimpleMeterRegistry();

    private final MicrometerMetricsReporter reporter = new TestReactiveStreamsMetricsReporter(meterRegistry);

    @Before
    public void setup() {
        reporter.configure(Collections.emptyMap());
    }

    @After
    public void teardown() {
        reporter.close();
    }

    @Test
    public void metricNamesAreSanitized() {
        KafkaMetric metric = createMetric("group", "topic_name-1.name", Collections.emptyMap(), Math::random);

        reporter.metricChange(metric);

        assertNotNull(meterRegistry.find("kafka.group.name").gauge());
    }

    @Test
    public void clientIdsAreSanitized() {
        Map<String, String> tags = Collections.singletonMap(MicrometerMetricsReporter.CLIENT_ID_TAG, "client-id-1");
        KafkaMetric metric = createMetric("group", "name", tags, Math::random);

        reporter.metricChange(metric);

        Tag expectedTag = Tag.of(MicrometerMetricsReporter.CLIENT_ID_TAG, "client-id");
        assertNotNull(meterRegistry.find("kafka.group.name").tags(Collections.singletonList(expectedTag)).gauge());
    }

    private KafkaMetric createMetric(String group, String name, Map<String, String> tags, Supplier<Double> metricValueSupplier) {
        MetricName metricName = new MetricName(name, group, "TEST", tags);
        Measurable measurable = (config, now) -> metricValueSupplier.get();
        return new KafkaMetric(this, metricName, measurable, new MetricConfig(), Time.SYSTEM);
    }

    private static final class TestReactiveStreamsMetricsReporter extends ReactiveStreamsMetricsReporter {

        private final MeterRegistry meterRegistry;

        private TestReactiveStreamsMetricsReporter(MeterRegistry meterRegistry) {
            this.meterRegistry = meterRegistry;
        }

        @Override
        protected MeterRegistry createMeterRegistry(Map<String, ?> configs) {
            return meterRegistry;
        }
    }
}