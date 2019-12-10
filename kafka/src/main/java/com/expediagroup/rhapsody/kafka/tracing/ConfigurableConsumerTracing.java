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
package com.expediagroup.rhapsody.kafka.tracing;

import java.util.Map;
import java.util.Objects;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.Configurable;

import com.expediagroup.rhapsody.core.tracing.ConfigurableTracing;
import com.expediagroup.rhapsody.kafka.extractor.ConsumerRecordExtraction;

import io.opentracing.tag.Tags;

public abstract class ConfigurableConsumerTracing extends ConfigurableTracing<ConsumerRecord> implements Configurable {

    public static final String HEADER_TAG_KEYS_CONFIG = "tracing.header.tag.keys";

    protected static final String REACTIVE_STREAMS_COMPONENT = "rhapsody-kafka";

    @Override
    protected Map<String, String> extractHeaders(ConsumerRecord record) {
        return ConsumerRecordExtraction.extractHeaderMap(record);
    }

    @Override
    protected Map<String, String> createTags(ConsumerRecord record, Map<String, String> extractedHeaders) {
        Map<String, String> tags = super.createTags(record, extractedHeaders);
        tags.put(Tags.COMPONENT.getKey(), REACTIVE_STREAMS_COMPONENT);
        tags.put(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CONSUMER);
        tags.put("topic", record.topic());
        tags.put("partition", Integer.toString(record.partition()));
        tags.put("offset", Long.toString(record.offset()));
        return tags;
    }

    @Override
    protected String getHeaderTagKeysConfig() {
        return HEADER_TAG_KEYS_CONFIG;
    }

    protected static String extractClientId(Map<String, ?> configs) {
        return Objects.requireNonNull(configs.get(CommonClientConfigs.CLIENT_ID_CONFIG)).toString();
    }
}
