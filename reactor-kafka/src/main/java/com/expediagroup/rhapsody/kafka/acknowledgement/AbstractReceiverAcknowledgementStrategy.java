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

import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.reactivestreams.Publisher;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.kafka.factory.AcknowledgeableConsumerRecordFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.kafka.receiver.ReceiverRecord;

abstract class AbstractReceiverAcknowledgementStrategy implements ReceiverAcknowledgementStrategy {

    // Note that transformation is deferred on a per-Subscription basis to avoid possible
    // complications due to lost Acknowledgers between Subscriptions
    @Override
    public final <K, V> Function<? super Publisher<ReceiverRecord<K, V>>, ? extends Publisher<Acknowledgeable<ConsumerRecord<K, V>>>>
    createRecordTransformer(Map<String, ?> properties) {
        AcknowledgeableConsumerRecordFactory<K, V> acknowledgeableFactory = AcknowledgeableConsumerRecordFactory.create(properties);
        long maxInFlight = ReceiverAcknowledgementStrategy.loadMaxInFlightPerSubscription(properties).orElse(Long.MAX_VALUE);
        return source -> Flux.defer(() -> transform(source, acknowledgeableFactory, maxInFlight));
    }

    protected final <K, V> Publisher<Acknowledgeable<ConsumerRecord<K, V>>>
    transform(Publisher<? extends ReceiverRecord<K, V>> source, AcknowledgeableConsumerRecordFactory<K, V> acknowledgeableFactory, long maxInFlight) {
        Sinks.Empty<Acknowledgeable<ConsumerRecord<K, V>>> sink = Sinks.empty();
        return Flux.<ReceiverRecord<K, V>>from(source)
            .map(receiverRecord -> acknowledgeableFactory.create(receiverRecord, receiverRecord.receiverOffset()::acknowledge, sink::tryEmitError))
            .mergeWith(sink.asMono())
            .transform(createOperator(maxInFlight));
    }

    protected abstract <K, V> UnaryOperator<Publisher<Acknowledgeable<ConsumerRecord<K, V>>>> createOperator(long maxInFlight);
}
