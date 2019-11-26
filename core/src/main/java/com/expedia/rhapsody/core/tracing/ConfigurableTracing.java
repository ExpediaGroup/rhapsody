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
package com.expedia.rhapsody.core.tracing;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.expedia.rhapsody.util.ConfigLoading;

import io.opentracing.References;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMapExtractAdapter;
import io.opentracing.util.GlobalTracer;

public abstract class ConfigurableTracing<T> {

    protected Tracer tracer = GlobalTracer.get();

    protected boolean referenceParentSpan = true;

    protected Set<String> headerTagKeys = Collections.emptySet();

    public void configure(Map<String, ?> configs) {
        this.tracer = createTracer(configs);
        this.referenceParentSpan = ConfigLoading.load(configs, getReferenceParentSpanConfig(), Boolean::valueOf, referenceParentSpan);
        this.headerTagKeys = ConfigLoading.loadCollection(configs, getHeaderTagKeysConfig(), Function.identity(), Collectors.toCollection(this::createCommonHeaderTagKeys))
            .orElseGet(this::createCommonHeaderTagKeys);
    }

    protected Tracer createTracer(Map<String, ?> configs) {
        return GlobalTracer.get();
    }

    protected void buildAndFinishSpan(T traced) {
        Map<String, String> extractedHeaders = extractHeaders(traced);
        tryExtractSpanContext(extractedHeaders).ifPresent(spanContext ->
            buildSpan(traced, extractedHeaders).addReference(References.FOLLOWS_FROM, spanContext).start().finish());
    }

    protected Tracer.SpanBuilder buildSpan(T traced) {
        Map<String, String> extractedHeaders = extractHeaders(traced);
        Optional<SpanContext> parentSpanToReference = referenceParentSpan ? tryExtractSpanContext(extractedHeaders) : Optional.empty();
        return parentSpanToReference
            .map(spanContext -> buildSpan(traced, extractedHeaders).addReference(References.FOLLOWS_FROM, spanContext))
            .orElseGet(() -> buildSpan(traced, extractedHeaders));
    }

    protected final Tracer.SpanBuilder buildSpan(T traced, Map<String, String> extractedHeaders) {
        Tracer.SpanBuilder spanBuilder = buildSpan();
        createTags(traced, extractedHeaders).forEach(spanBuilder::withTag);
        return spanBuilder;
    }

    protected final Tracer.SpanBuilder buildSpan() {
        return buildSpan(true);
    }

    protected final Tracer.SpanBuilder buildSpan(boolean ignoreActive) {
        return ignoreActive ? tracer.buildSpan(getOperationName()).ignoreActiveSpan() : tracer.buildSpan(getOperationName());
    }

    protected Set<String> createCommonHeaderTagKeys() {
        return new HashSet<>();
    }

    protected abstract Map<String, String> extractHeaders(T traced);

    protected Map<String, String> createTags(T traced, Map<String, String> extractedHeaders) {
        return extractHeaderTags(extractedHeaders);
    }

    protected final Map<String, String> extractHeaderTags(Map<String, String> extractedHeaders) {
        Map<String, String> headerTags = new HashMap<>();
        headerTagKeys.forEach(headerTagKey -> {
            if (extractedHeaders.get(headerTagKey) != null) {
                headerTags.put(headerTagKey, extractedHeaders.get(headerTagKey));
            }
        });
        return headerTags;
    }

    protected abstract String getReferenceParentSpanConfig();

    protected abstract String getHeaderTagKeysConfig();

    protected abstract String getOperationName();

    protected final Map<String, String> extractBaggage(Map<String, String> headers, Map<String, String> tags, SpanContext spanContext) {
        Map<String, String> baggage = extractBaggage(headers, tags);
        tracer.inject(spanContext, Format.Builtin.TEXT_MAP, new InverseTextMapInjectAdapter(baggage));
        return baggage;
    }

    protected final Map<String, String> extractBaggage(Map<String, String> headers, Map<String, String> tags) {
        Map<String, String> baggage = new HashMap<>(headers);
        tags.keySet().forEach(baggage::remove);
        return baggage;
    }

    private Optional<SpanContext> tryExtractSpanContext(Map<String, String> headers) {
        return Optional.ofNullable(tracer.extract(Format.Builtin.TEXT_MAP, new TextMapExtractAdapter(headers)));
    }
}
