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
package com.expediagroup.rhapsody.kafka.acknowledgement;

import java.util.function.UnaryOperator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.core.acknowledgement.OrderManagingAcknowledgementOperator;
import com.expediagroup.rhapsody.kafka.extractor.ConsumerRecordExtraction;

public final class OrderManagingReceiverAcknowledgementStrategy extends AbstractReceiverAcknowledgementStrategy {

    @Override
    protected <K, V> UnaryOperator<Publisher<Acknowledgeable<ConsumerRecord<K, V>>>> createOperator(long maxInFlight) {
        return publisher -> new OrderManagingAcknowledgementOperator<>(publisher, ConsumerRecordExtraction::extractTopicPartition, maxInFlight);
    }
}
