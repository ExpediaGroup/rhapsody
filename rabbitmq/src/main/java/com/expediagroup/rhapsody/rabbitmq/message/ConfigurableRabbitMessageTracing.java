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

import java.util.Map;

import com.expediagroup.rhapsody.core.tracing.ConfigurableTracing;

import io.opentracing.tag.Tags;

public abstract class ConfigurableRabbitMessageTracing<T> extends ConfigurableTracing<RabbitMessage<T>> {

    public static final String HEADER_TAG_KEYS_CONFIG = "tracing-header-tag-keys";

    private static final String REACTIVE_STREAMS_COMPONENT = "rhapsody-rabbitmq";

    @Override
    protected Map<String, String> extractHeaders(RabbitMessage<T> rabbitMessage) {
        return RabbitMessageExtraction.extractStringifiedHeaders(rabbitMessage);
    }

    @Override
    protected Map<String, String> createTags(RabbitMessage<T> rabbitMessage, Map<String, String> extractedHeaders) {
        Map<String, String> tags = super.createTags(rabbitMessage, extractedHeaders);
        tags.put(Tags.COMPONENT.getKey(), REACTIVE_STREAMS_COMPONENT);
        tags.put(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);
        tags.put("routingKey", rabbitMessage.getRoutingKey());
        return tags;
    }

    @Override
    protected String getHeaderTagKeysConfig() {
        return HEADER_TAG_KEYS_CONFIG;
    }
}
