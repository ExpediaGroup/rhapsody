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
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.kafka.sending.AcknowledgeableSenderResult;
import com.expediagroup.rhapsody.util.ConfigLoading;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

public class KafkaSenderFactory<K, V> {

    public static final String MAX_IN_FLIGHT_CONFIG = "max.in.flight";

    public static final String HEADERS_ENABLED_CONFIG = "headers.enabled";

    public static final String STOP_ON_ERROR_CONFIG = "stop.on.error";

    public static final String RESUBSCRIBE_ON_ERROR_CONFIG = "resubscribe.on.error";

    private static final boolean DEFAULT_HEADERS_ENABLED = false;

    private static final boolean DEFAULT_STOP_ON_ERROR = false;

    private static final boolean DEFAULT_RESUBSCRIBE_ON_ERROR = true;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSenderFactory.class);

    private static final Map<String, AtomicLong> REGISTRATION_COUNTS_BY_CLIENT_ID = new ConcurrentHashMap<>();

    private final KafkaSender<K, V> kafkaSender;

    private final boolean headersEnabled;

    private final boolean resubscribeOnError;

    public KafkaSenderFactory(KafkaConfigFactory configFactory) {
        Map<String, Object> properties = configFactory.create();
        this.kafkaSender = KafkaSender.create(createSenderOptions(properties));
        this.headersEnabled = ConfigLoading.load(properties, HEADERS_ENABLED_CONFIG, Boolean::valueOf, DEFAULT_HEADERS_ENABLED);
        this.resubscribeOnError = ConfigLoading.load(properties, RESUBSCRIBE_ON_ERROR_CONFIG, Boolean::valueOf, DEFAULT_RESUBSCRIBE_ON_ERROR);
    }

    public Function<Publisher<Acknowledgeable<ProducerRecord<K, V>>>, Flux<Acknowledgeable<SenderResult<V>>>> sendAcknowledgeable() {
        return this::sendAcknowledgeable;
    }

    public Flux<Acknowledgeable<SenderResult<V>>> sendAcknowledgeable(Publisher<Acknowledgeable<ProducerRecord<K, V>>> acknowledgeables) {
        return Flux.from(acknowledgeables)
            .map(this::createAcknowledgeableValuedSenderRecord)
            .transform(this::sendRecords)
            .map(AcknowledgeableSenderResult::fromSenderResult);
    }

    public Function<Publisher<ProducerRecord<K, V>>, Flux<SenderResult<V>>> send() {
        return this::send;
    }

    public Flux<SenderResult<V>> send(Publisher<ProducerRecord<K, V>> producerRecords) {
        return Flux.from(producerRecords)
            .map(this::createValuedSenderRecord)
            .transform(this::sendRecords);
    }

    private SenderOptions<K, V> createSenderOptions(Map<String, Object> properties) {
        SenderOptions<K, V> senderOptions = SenderOptions.create(properties);

        // Client IDs must be made unique in order to avoid conflicting registration with external
        // resources, i.e. JMX.
        String uniqueClientId = registerNewClient(Objects.toString(senderOptions.producerProperty(CommonClientConfigs.CLIENT_ID_CONFIG)));
        senderOptions.producerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, uniqueClientId);

        // Publish SenderResults on a dedicated-and-identifiable non-daemon Scheduler
        senderOptions.scheduler(Schedulers.newSingle(KafkaSenderFactory.class.getSimpleName() + "-" + uniqueClientId));

        // This is the maximum number of "in-flight" Records per sent Publisher. Note that this is
        // different from ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, which controls the
        // total number of in-flight Requests for all sent Publishers on a Sender/Producer. It is
        // important to note that if this is greater than 1 and a fatal error/cancellation occurs,
        // the SenderResults of other in-flight Records will NOT be delivered downstream, removing
        // the ability to execute auxiliary tasks on those Results, i.e. recovery from error or
        // execution of acknowledgement
        senderOptions.maxInFlight(ConfigLoading.load(properties, MAX_IN_FLIGHT_CONFIG, Integer::valueOf, senderOptions.maxInFlight()));

        // Upon occurrence of synchronous Exceptions (i.e. serialization), sent Publishers are
        // immediately fatally errored/canceled. Upon asynchronous Exceptions (i.e. network issue),
        // Reactor allows configuring whether or not to "stop" (aka error-out) the sent Publisher.
        // Therefore, if this is disabled, there can be slightly different behavior for synchronous
        // vs. asynchronous Exceptions. Most commonly, with non-singleton Publishers and with Error
        // Resubscription enabled, the difference in behavior is directly related to the max number
        // of in-flight Records, i.e. if the max in-flight Records is 1, there is no difference
        // between this being enabled or disabled. If the max in-flight messages is greater than 1,
        // other in-flight SenderResults may not be delivered downstream if this is enabled.
        senderOptions.stopOnError(ConfigLoading.load(properties, STOP_ON_ERROR_CONFIG, Boolean::valueOf, DEFAULT_STOP_ON_ERROR));

        return senderOptions;
    }

    private <R> Flux<SenderResult<R>> sendRecords(Flux<SenderRecord<K, V, R>> senderRecords) {
        return senderRecords
            .transform(kafkaSender::send)
            .doOnError(error -> LOGGER.warn("An Error was encountered while trying to send to Kafka. resubscribeOnError={}", resubscribeOnError, error))
            .retry(error -> resubscribeOnError);
    }

    private SenderRecord<K, V, Acknowledgeable<V>> createAcknowledgeableValuedSenderRecord(Acknowledgeable<ProducerRecord<K, V>> acknowledgeable) {
        return createSenderRecord(acknowledgeable.get(), acknowledgeable.map(ProducerRecord::value));
    }

    private SenderRecord<K, V, V> createValuedSenderRecord(ProducerRecord<K, V> producerRecord) {
        return createSenderRecord(producerRecord, producerRecord.value());
    }

    // This method helps work around issues of pre-0.11.0.0 Kafka Client users and/or usage with
    // with Kafka Brokers/Topics that use `log.message.format.version` < 0.11.0.0. Headers
    // enablement defaults to false in order to not break backward-compatibility
    private <T> SenderRecord<K, V, T> createSenderRecord(ProducerRecord<K, V> record, T correlationMetadata) {
        return headersEnabled || !record.headers().iterator().hasNext() ?
            SenderRecord.create(record, correlationMetadata) :
            SenderRecord.create(new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), record.value()), correlationMetadata);
    }

    private static String registerNewClient(String clientId) {
        return clientId + "-" + REGISTRATION_COUNTS_BY_CLIENT_ID.computeIfAbsent(clientId, key -> new AtomicLong()).incrementAndGet();
    }
}
