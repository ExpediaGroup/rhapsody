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

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.core.transformer.AutoAcknowledgementConfig;
import com.expediagroup.rhapsody.core.transformer.AutoAcknowledgingTransformer;
import com.expediagroup.rhapsody.kafka.acknowledgement.MultipleReceiverAcknowledgementStrategy;
import com.expediagroup.rhapsody.kafka.acknowledgement.OrderManagingReceiverAcknowledgementStrategy;
import com.expediagroup.rhapsody.kafka.acknowledgement.ReceiverAcknowledgementStrategy;
import com.expediagroup.rhapsody.kafka.extractor.ConsumerRecordExtraction;
import com.expediagroup.rhapsody.util.ConfigLoading;
import com.expediagroup.rhapsody.util.Defaults;

import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverPartition;

public class KafkaFluxFactory<K, V> {

    public static final String POLL_TIMEOUT_CONFIG = "poll.timeout";

    public static final String CLOSE_TIMEOUT_CONFIG = "close.timeout";

    public static final String MAX_COMMIT_ATTEMPTS_CONFIG = "max.commit.attempts";

    public static final String BLOCK_REQUEST_ON_PARTITION_ASSIGNMENT_CONFIG = "block.request.on.partition.assignment";

    private static final Duration DEFAULT_CLOSE_TIMEOUT = Duration.ofSeconds(30L);

    private static final boolean DEFAULT_BLOCK_REQUEST_ON_PARTITION_ASSIGNMENT = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaFluxFactory.class);

    private static final Map<String, AtomicLong> REGISTRATION_COUNTS_BY_CLIENT_ID = new ConcurrentHashMap<>();

    private final KafkaConfigFactory configFactory;

    public KafkaFluxFactory(KafkaConfigFactory configFactory) {
        this.configFactory = configFactory;
    }

    public Flux<GroupedFlux<TopicPartition, ConsumerRecord<K, V>>> receiveAutoGroup(Collection<String> topics,
        AutoAcknowledgementConfig autoAcknowledgementConfig,
        Function<? super Flux<ConsumerRecord<K, V>>, ? extends Publisher<ConsumerRecord<K, V>>> pregroup) {
        return receiveAuto(topics, autoAcknowledgementConfig)
            .transform(pregroup)
            .groupBy(ConsumerRecordExtraction::extractTopicPartition);
    }

    public Flux<ConsumerRecord<K, V>> receiveAuto(Collection<String> topics, AutoAcknowledgementConfig autoAcknowledgementConfig) {
        return receive(topics, new MultipleReceiverAcknowledgementStrategy())
            .transformDeferred(new AutoAcknowledgingTransformer<>(autoAcknowledgementConfig, KafkaFluxFactory::collectAcknowledgers, KafkaFluxFactory::acknowledge))
            .map(Acknowledgeable::get);
    }

    public Flux<GroupedFlux<TopicPartition, Acknowledgeable<ConsumerRecord<K, V>>>> receiveGroup(Collection<String> topics,
        ReceiverAcknowledgementStrategy acknowledgementStrategy,
        Function<? super Flux<Acknowledgeable<ConsumerRecord<K, V>>>, ? extends Publisher<Acknowledgeable<ConsumerRecord<K, V>>>> pregroup) {
        return receive(topics, acknowledgementStrategy)
            .transform(pregroup)
            .groupBy(KafkaFluxFactory::extractTopicPartition);
    }

    public Flux<Acknowledgeable<ConsumerRecord<K, V>>> receive(Collection<String> topics) {
        return receive(topics, new OrderManagingReceiverAcknowledgementStrategy());
    }

    public Flux<Acknowledgeable<ConsumerRecord<K, V>>> receive(Collection<String> topics, ReceiverAcknowledgementStrategy acknowledgementStrategy) {
        Map<String, Object> properties = configFactory.create();

        ReceiverOptions<K, V> receiverOptions = ReceiverOptions.create(properties);

        // Reactor allows controlling the timeout of its polls to Kafka. This config can be
        // increased if the Kafka cluster is slow to respond.
        receiverOptions.pollTimeout(ConfigLoading.load(properties, POLL_TIMEOUT_CONFIG, Duration::parse, receiverOptions.pollTimeout()));

        // Closing the underlying Kafka Consumer is a fallible process. In order to not infinitely
        // deadlock a Consumer during this process (which can lead to non-consumption of assigned
        // partitions), we use a default equal to what's used in KafkaConsumer::close
        receiverOptions.closeTimeout(ConfigLoading.load(properties, CLOSE_TIMEOUT_CONFIG, Duration::parse, DEFAULT_CLOSE_TIMEOUT));

        // Reactor takes control of offset committing by disabling the native Kafka auto commit
        // and periodically committing offsets of acknowledged Records. Since the native
        // auto-commit is disabled, we are reusing the native property used to configure offset
        // commit intervals.
        receiverOptions.commitInterval(
            ConfigLoading.load(properties, ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, Long::valueOf)
                .map(Duration::ofMillis)
                .orElse(receiverOptions.commitInterval()));

        // Committing Offsets can fail for retriable reasons. This config can be increased if
        // failing to commit offsets is found to be particularly frequent
        receiverOptions.maxCommitAttempts(ConfigLoading.load(properties, MAX_COMMIT_ATTEMPTS_CONFIG, Integer::valueOf, receiverOptions.maxCommitAttempts()));

        // Subscribers may want to block request Threads on assignment of partitions AND subsequent
        // fetching/updating of offset positions on those partitions such that all imminently
        // produced Records to the subscribed Topics will be received by the associated Consumer
        // Group. This can help avoid timing problems, particularly with tests, and avoids having
        // to use `auto.offset.reset = "earliest"` to guarantee receipt of Records immediately
        // produced by the request Thread (directly or indirectly)
        CompletableFuture<Collection<ReceiverPartition>> assignedPartitions =
            ConfigLoading.load(properties, BLOCK_REQUEST_ON_PARTITION_ASSIGNMENT_CONFIG, Boolean::valueOf, DEFAULT_BLOCK_REQUEST_ON_PARTITION_ASSIGNMENT) ?
                new CompletableFuture<>() : CompletableFuture.completedFuture(Collections.emptyList());
        Future<?> assignedPartitionPositions = assignedPartitions.thenAccept(partitions -> partitions.forEach(ReceiverPartition::position));
        receiverOptions.addAssignListener(assignedPartitions::complete);

        // Use a dedicated and identifiable Scheduler for publishing
        String schedulerName = KafkaFluxFactory.class.getSimpleName() + "-" + extractClientId(receiverOptions);
        Scheduler scheduler = Schedulers.newBoundedElastic(Defaults.THREAD_CAP, Integer.MAX_VALUE, schedulerName);
        receiverOptions.schedulerSupplier(() -> scheduler);

        // 1) KafkaReceivers are not thread-safe and therefore must not be shared among
        //    Subscriptions. We therefore defer KafkaReceiver creation on a per-Subscription basis
        // 2) Every time a KafkaReceiver is created for a new Subscription, its Consumer's Client
        //    ID must be made unique in order to avoid conflicting registration with external
        //    resources, i.e. JMX. Since we don't want to recreate the Properties from the
        //    ConfigFactory (and/or reparse those Properties) on every subscription AND we want to
        //    maintain thread safety, we create a "unique" ReceiverOptions from the base Options
        // 3) Subscribe to Kafka Records first in order to trigger polling and then block
        //    requesting Thread on assigned partition positions iff necessary
        return Flux.defer(() -> KafkaReceiver.create(createUniqueReceiverOptions(receiverOptions, topics)).receive())
            .transform(acknowledgementStrategy.<K, V>createRecordTransformer(properties))
            .transform(records -> assignedPartitionPositions.isDone() ? records : records.mergeWith(blockRequestOn(assignedPartitionPositions)));
    }

    private static <K, V> ReceiverOptions<K, V> createUniqueReceiverOptions(ReceiverOptions<K, V> receiverOptions, Collection<String> topics) {
        ReceiverOptions<K, V> uniqueReceiverOptions = ReceiverOptions.<K, V>create(receiverOptions.consumerProperties())
            .consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG, registerNewClient(extractClientId(receiverOptions)))
            .pollTimeout(receiverOptions.pollTimeout())
            .closeTimeout(receiverOptions.closeTimeout())
            .commitInterval(receiverOptions.commitInterval())
            .maxCommitAttempts(receiverOptions.maxCommitAttempts())
            .schedulerSupplier(receiverOptions.schedulerSupplier())
            .subscription(topics);
        receiverOptions.assignListeners().forEach(uniqueReceiverOptions::addAssignListener);
        return uniqueReceiverOptions;
    }

    private static <T> Mono<T> blockRequestOn(Future<?> future) {
        return Mono.<T>empty().doOnRequest(requested -> {
            try {
                future.get();
            } catch (Exception e) {
                LOGGER.error("Failed to block Request Thread on Future", e);
            }
        });
    }

    private static <K, V> Mono<Map<TopicPartition, Runnable>> collectAcknowledgers(Flux<Acknowledgeable<ConsumerRecord<K, V>>> acknowledgeables) {
        return acknowledgeables.collectMap(KafkaFluxFactory::extractTopicPartition, Acknowledgeable::getAcknowledger);
    }

    private static void acknowledge(Map<TopicPartition, Runnable> acknowledgers) {
        acknowledgers.values().forEach(Runnable::run);
    }

    private static <K, V> TopicPartition extractTopicPartition(Acknowledgeable<ConsumerRecord<K, V>> acknowledgeable) {
        return ConsumerRecordExtraction.extractTopicPartition(acknowledgeable.get());
    }

    private static String extractClientId(ReceiverOptions receiverOptions) {
        return Objects.toString(receiverOptions.consumerProperty(CommonClientConfigs.CLIENT_ID_CONFIG));
    }

    private static String registerNewClient(String clientId) {
        return clientId + "-" + REGISTRATION_COUNTS_BY_CLIENT_ID.computeIfAbsent(clientId, key -> new AtomicLong()).incrementAndGet();
    }
}
