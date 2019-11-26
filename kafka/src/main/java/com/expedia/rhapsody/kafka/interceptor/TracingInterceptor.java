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
package com.expedia.rhapsody.kafka.interceptor;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expedia.rhapsody.kafka.record.RecordHeaderConversion;
import com.expedia.rhapsody.kafka.tracing.ConfigurableConsumerTracing;
import com.expedia.rhapsody.kafka.tracing.UniqueHeadersTextMapInjectAdapter;

import io.opentracing.Scope;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.tag.Tags;

public class TracingInterceptor<K, V> extends ConfigurableConsumerTracing implements ProducerInterceptor<K, V>, ConsumerInterceptor<K, V> {

    public static final String REFERENCE_PARENT_SPAN_CONFIG = "tracing.interceptor.reference.parent.span";

    private static final Logger LOGGER = LoggerFactory.getLogger(TracingInterceptor.class);

    private String operationName = "intercept";

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        this.operationName = (isConsumerConfig(configs) ? "consume" : "produce") + "-intercept-" + extractClientId(configs);
    }

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        Map<String, String> headers = RecordHeaderConversion.toMap(record.headers());

        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP, new TextMapExtractAdapter(headers));
        Map<String, String> tags = createTags(record, headers);
        Map<String, String> baggage = spanContext != null ? extractBaggage(headers, tags, spanContext) : extractBaggage(headers, tags);

        Tracer.SpanBuilder spanBuilder = spanContext != null && referenceParentSpan ? buildSpan().asChildOf(spanContext) : buildSpan(!referenceParentSpan);
        tags.forEach(spanBuilder::withTag);

        try (Scope scope = spanBuilder.startActive(true)) {
            baggage.forEach(scope.span()::setBaggageItem);
            injectSpanContext(record, scope.span().context());
            return record;
        }
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        records.forEach(this::buildAndFinishSpan);
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void close() {

    }

    @Override
    protected String getOperationName() {
        return operationName;
    }

    @Override
    protected String getReferenceParentSpanConfig() {
        return REFERENCE_PARENT_SPAN_CONFIG;
    }

    protected Map<String, String> createTags(ProducerRecord record, Map<String, String> extractedHeaders) {
        Map<String, String> tags = extractHeaderTags(extractedHeaders);
        tags.put(Tags.COMPONENT.getKey(), REACTIVE_STREAMS_COMPONENT);
        tags.put(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_PRODUCER);
        tags.put(Tags.MESSAGE_BUS_DESTINATION.getKey(), record.topic());
        return tags;
    }

    private void injectSpanContext(ProducerRecord<K, V> record, SpanContext spanContext) {
        try {
            tracer.inject(spanContext, Format.Builtin.TEXT_MAP, new UniqueHeadersTextMapInjectAdapter(record.headers()));
        } catch (Exception e) {
            LOGGER.warn("Failed to inject SpanContext in to ProducerRecord Headers. This COULD be a resend...", e);
        }
    }

    private static boolean isConsumerConfig(Map<String, ?> configs) {
        return configs.get(ConsumerConfig.GROUP_ID_CONFIG) != null;
    }
}
