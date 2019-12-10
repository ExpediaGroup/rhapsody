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
package com.expediagroup.rhapsody.rabbitmq.message;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.AMQP;

import io.opentracing.Scope;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.propagation.TextMapInjectAdapter;

public class TracingRabbitMessageSendInterceptor<T> extends ConfigurableRabbitMessageTracing<T> implements RabbitMessageSendInterceptor<T> {

    public static final String REFERENCE_PARENT_SPAN_CONFIG = "tracing-interceptor-reference-parent-span";

    @Override
    public RabbitMessage<T> onSend(RabbitMessage<T> rabbitMessage) {
        Map<String, String> headers = RabbitMessageExtraction.extractStringifiedHeaders(rabbitMessage);

        SpanContext spanContext = tracer.extract(Format.Builtin.TEXT_MAP, new TextMapExtractAdapter(headers));
        Map<String, String> tags = createTags(rabbitMessage, headers);
        Map<String, String> baggage = spanContext != null ? extractBaggage(headers, tags, spanContext) : extractBaggage(headers, tags);

        Tracer.SpanBuilder spanBuilder = spanContext != null && referenceParentSpan ? buildSpan().asChildOf(spanContext) : buildSpan(!referenceParentSpan);
        tags.forEach(spanBuilder::withTag);

        try (Scope scope = spanBuilder.startActive(true)) {
            baggage.forEach(scope.span()::setBaggageItem);
            return injectSpanContext(rabbitMessage, scope.span().context());
        }
    }

    @Override
    protected String getReferenceParentSpanConfig() {
        return REFERENCE_PARENT_SPAN_CONFIG;
    }

    @Override
    protected String getOperationName() {
        return "create-rabbitmessage";
    }

    private RabbitMessage<T> injectSpanContext(RabbitMessage<T> rabbitMessage, SpanContext context) {
        Map<String, String> injectedContext = new HashMap<>();
        tracer.inject(context, Format.Builtin.TEXT_MAP, new TextMapInjectAdapter(injectedContext));

        AMQP.BasicProperties properties = rabbitMessage.getProperties();
        Map<String, Object> headers = new HashMap<>(properties.getHeaders() == null ? Collections.emptyMap() : properties.getHeaders());
        headers.putAll(injectedContext);

        return new RabbitMessage<>(rabbitMessage.getExchange(), rabbitMessage.getRoutingKey(), properties.builder().headers(headers).build(), rabbitMessage.getBody());
    }
}
