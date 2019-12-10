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
package com.expediagroup.rhapsody.kafka.factory;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.core.tracing.TracingDecoration;
import com.expediagroup.rhapsody.kafka.tracing.ConfigurableConsumerTracing;
import com.expediagroup.rhapsody.kafka.tracing.TracingAcknowledgeableConsumerRecord;

import io.opentracing.Span;

public class TracingAcknowledgeableConsumerRecordFactory<K, V> extends ConfigurableConsumerTracing implements AcknowledgeableConsumerRecordFactory<K, V> {

    public static final String REFERENCE_PARENT_SPAN_CONFIG = "tracing.acknowledgeable.reference.parent.span";

    private static final String OPERATION_NAME_PREFIX = "consume-acknowledgeable";

    private String operationName = OPERATION_NAME_PREFIX;

    @Override
    public void configure(Map<String, ?> configs) {
        super.configure(configs);
        this.operationName = OPERATION_NAME_PREFIX + "-" + extractClientId(configs);
    }

    @Override
    public Acknowledgeable<ConsumerRecord<K, V>> create(ConsumerRecord<K, V> record, Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        Span span = buildSpan(record).start();
        Runnable decoratedAcknowledger = TracingDecoration.decorateAcknowledger(span, acknowledger);
        Consumer<? super Throwable> decoratedNacknowledger = TracingDecoration.decorateNacknowledger(span, nacknowledger);
        return new TracingAcknowledgeableConsumerRecord<>(tracer, span, record, decoratedAcknowledger, decoratedNacknowledger);
    }

    @Override
    protected String getOperationName() {
        return operationName;
    }

    @Override
    protected String getReferenceParentSpanConfig() {
        return REFERENCE_PARENT_SPAN_CONFIG;
    }
}
