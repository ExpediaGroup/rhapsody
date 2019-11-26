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

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingInterceptor<K, V> implements ProducerInterceptor<K, V>, ConsumerInterceptor<K, V> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingInterceptor.class);

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        LOGGER.info("Producer is sending: topic={} partition={} key={}", record.topic(), record.partition(), record.key());
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata == null) {
            LOGGER.warn("Producer RecordMetadata is NULL. Producer#send must have received a NULL value");
        } else if (exception != null) {
            LOGGER.warn("Producer Exception: topic={} e={}", metadata.topic(), exception);
        } else {
            LOGGER.info("Producer Ack: topic={} partition={} offset={}", metadata.topic(), metadata.partition(), metadata.offset());
        }
    }

    @Override
    public ConsumerRecords<K, V> onConsume(ConsumerRecords<K, V> records) {
        records.forEach(record -> LOGGER.info("Consumer received Record: topic={} partition={} offset={} key={}",
            record.topic(), record.partition(), record.offset(), record.key()));
        return records;
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((topicPartition, offsetAndMetadata) -> LOGGER.info("Consumer committed Offset: topic={} partition={} offset={}",
            topicPartition.topic(), topicPartition.partition(), offsetAndMetadata.offset()));
    }

    @Override
    public void close() {

    }
}
