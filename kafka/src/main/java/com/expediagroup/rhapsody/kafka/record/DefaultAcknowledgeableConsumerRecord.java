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
package com.expediagroup.rhapsody.kafka.record;

import java.util.function.Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.expediagroup.rhapsody.api.AbstractAcknowledgeable;
import com.expediagroup.rhapsody.api.AcknowledgeableFactory;
import com.expediagroup.rhapsody.api.ComposedAcknowledgeable;
import com.expediagroup.rhapsody.api.Header;
import com.expediagroup.rhapsody.kafka.extractor.ConsumerRecordExtraction;

public class DefaultAcknowledgeableConsumerRecord<K, V> extends AbstractAcknowledgeable<ConsumerRecord<K, V>> {

    private final ConsumerRecord<K, V> consumerRecord;

    private final Runnable acknowledger;

    private final Consumer<? super Throwable> nacknowledger;

    public DefaultAcknowledgeableConsumerRecord(ConsumerRecord<K, V> consumerRecord, Runnable acknowledger, Consumer<? super Throwable> nacknowledger) {
        this.consumerRecord = consumerRecord;
        this.acknowledger = acknowledger;
        this.nacknowledger = nacknowledger;
    }

    @Override
    public Header header() {
        return Header.fromMap(ConsumerRecordExtraction.extractHeaderMap(consumerRecord));
    }

    @Override
    public ConsumerRecord<K, V> get() {
        return consumerRecord;
    }

    @Override
    public Runnable getAcknowledger() {
        return acknowledger;
    }

    @Override
    public Consumer<? super Throwable> getNacknowledger() {
        return nacknowledger;
    }

    @Override
    protected <R> AcknowledgeableFactory<R> createPropagator() {
        return ComposedAcknowledgeable::new;
    }
}
