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

import java.time.Instant;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

import com.expedia.rhapsody.util.ConfigLoading;

public class LatestExceptionInterceptor<K, V> implements ProducerInterceptor<K, V>, ConsumerInterceptor<K, V> {

    private static final InterceptorRegistry<LatestExceptionInterceptor> REGISTRY = new InterceptorRegistry<>();

    private String clientId;

    private ExceptionTimestamp latestExceptionTimestamp;

    @Override
    public void configure(Map<String, ?> configs) {
        this.clientId = ConfigLoading.loadOrThrow(configs, CommonClientConfigs.CLIENT_ID_CONFIG, Function.identity());
        REGISTRY.registerIfAbsent(clientId, this);
    }

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        latestExceptionTimestamp = exception == null ? null : new ExceptionTimestamp(exception, extractTimestamp(metadata), metadata.toString());
    }

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void close() {
        REGISTRY.unregister(clientId);
    }

    public static InterceptorRegistry<LatestExceptionInterceptor> registry() {
        return REGISTRY;
    }

    public boolean hasException() {
        return getLatestExceptionTimestamp() != null;
    }

    public ExceptionTimestamp getLatestExceptionTimestamp() {
        return latestExceptionTimestamp;
    }

    private Long extractTimestamp(RecordMetadata metadata) {
        return metadata.timestamp() < 0 ? Instant.now().toEpochMilli() : metadata.timestamp();
    }
}
