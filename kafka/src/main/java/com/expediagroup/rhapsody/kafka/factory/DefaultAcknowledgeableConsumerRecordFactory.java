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
import com.expediagroup.rhapsody.kafka.record.DefaultAcknowledgeableConsumerRecord;

public class DefaultAcknowledgeableConsumerRecordFactory<K, V> implements AcknowledgeableConsumerRecordFactory<K, V> {

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public Acknowledgeable<ConsumerRecord<K, V>> create(ConsumerRecord<K, V> consumerRecord, Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        return new DefaultAcknowledgeableConsumerRecord<>(consumerRecord, acknowledger, nacknowledger);
    }
}
