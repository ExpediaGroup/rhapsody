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
package com.expediagroup.rhapsody.samples.opentracing;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.expediagroup.rhapsody.api.Acknowledgeable;
import com.expediagroup.rhapsody.kafka.acknowledgement.OrderManagingReceiverAcknowledgementStrategy;
import com.expediagroup.rhapsody.kafka.factory.AcknowledgeableConsumerRecordFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaConfigFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaSenderFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueFluxFactory;
import com.expediagroup.rhapsody.kafka.factory.KafkaValueSenderFactory;
import com.expediagroup.rhapsody.kafka.factory.TracingAcknowledgeableConsumerRecordFactory;
import com.expediagroup.rhapsody.kafka.interceptor.TracingInterceptor;
import com.expediagroup.rhapsody.kafka.test.TestKafkaFactory;

import io.opentracing.Span;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.GlobalTracer;
import reactor.core.publisher.Flux;

/**
 * This example shows how Rhapsody integrates with OpenTracing to provide tracing contexts around
 * the consumption and production of Kafka Records, as well as subsequent stream processing stages.
 */
public class KafkaOpenTracing {

    private static final Map<String, ?> TEST_KAFKA_CONFIG = new TestKafkaFactory().createKafka();

    private static final String TOPIC_1 = "TOPIC_1";

    private static final String TOPIC_2 = "TOPIC_2";

    public static void main(String[] args) throws Exception {
        //Step 1) Create Kafka Producer Config for Producer that backs Sender's Subscriber
        //implementation. Note we are adding a "Tracing" Interceptor which will propagate Span
        //Context from active Spans or from a new Span if one is not active. Note that we also
        //have to enable Header propagation if we want parent Span Context to be propagated in
        //the asynchronous streaming process
        KafkaConfigFactory kafkaSubscriberConfig = new KafkaConfigFactory();
        kafkaSubscriberConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaSubscriberConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaOpenTracing.class.getSimpleName());
        kafkaSubscriberConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaSubscriberConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        kafkaSubscriberConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        kafkaSubscriberConfig.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingInterceptor.class.getName());
        kafkaSubscriberConfig.put(KafkaSenderFactory.HEADERS_ENABLED_CONFIG, true);

        //Step 2) Create Kafka Consumer Config for Consumer that backs Receiver's Publisher
        //implementation. Note that we use an Auto Offset Reset of 'earliest' to ensure we receive
        //Records produced before subscribing with our new consumer group. Also note that we are
        //setting a Factory with which to create Acknowledgeable ConsumerRecords with OpenTracing
        //decoration. This decoration will cause Span eventing/logging around notable operations
        //as acknowledgement is propagated downstream
        KafkaConfigFactory kafkaPublisherConfig = new KafkaConfigFactory();
        kafkaPublisherConfig.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, TestKafkaFactory.extractConnect(TEST_KAFKA_CONFIG));
        kafkaPublisherConfig.put(CommonClientConfigs.CLIENT_ID_CONFIG, KafkaOpenTracing.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaOpenTracing.class.getSimpleName());
        kafkaPublisherConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaPublisherConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaPublisherConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaPublisherConfig.put(AcknowledgeableConsumerRecordFactory.PROPERTY, TracingAcknowledgeableConsumerRecordFactory.class.getName());

        //Step 3) Install a Tracer to the GlobalTracer registry, which the above configured
        //Tracing resources use to access Tracers by default
        MockTracer tracer = new MockTracer();
        GlobalTracer.register(tracer);

        //Step 4) Create a Value Sender Factory which we'll reuse to produce Records
        KafkaValueSenderFactory<String> senderFactory = new KafkaValueSenderFactory<>(kafkaSubscriberConfig);

        //Step 5) Start and activate a Span around which we'll log values to Kafka, followed by
        //Span completion (so it will show up as a completed Span later on)
        Span span = tracer.buildSpan(KafkaOpenTracing.class.getSimpleName()).start();
        tracer.scopeManager().activate(span, false);
        senderFactory
            .sendValues(Flux.just("Test"), value -> TOPIC_1, Function.identity())
            .then().block();
        span.finish();

        //Step 6) Apply consumption of the Kafka topic we've produced data to as a stream process.
        //The "process" in this stream upper-cases the values we sent previously, producing the
        //result to another topic. This portion also adheres to the responsibilities obliged by the
        //consumption of Acknowledgeable data. Note that we again need to explicitly limit the
        //number of results we expect ('.take(1)'), or else this Flow would never complete
        new KafkaValueFluxFactory<String>(kafkaPublisherConfig)
            .receiveValue(Collections.singletonList(TOPIC_1), new OrderManagingReceiverAcknowledgementStrategy())
            .map(Acknowledgeable.mapping(String::toUpperCase))
            .transform(upperCasedValues -> senderFactory.sendAcknowledgeableValues(upperCasedValues, value -> TOPIC_2, Function.identity()))
            .doOnNext(Acknowledgeable::acknowledge)
            .map(Acknowledgeable::get)
            .take(1)
            .collectList()
            .doOnNext(processedSenderResults -> System.out.println("processedSenderResults: " + processedSenderResults))
            .block();

        //Before printing, sort the finished Spans by start time (then ID) such that they are
        //easier to read below
        List<MockSpan> sortedSpans = tracer.finishedSpans().stream()
            .sorted(Comparator.comparing(MockSpan::startMicros).thenComparing(mockSpan -> mockSpan.context().spanId()))
            .collect(Collectors.toList());

        //The following should show a total of four Spans, all with the same Trace ID of the Span
        //we initially started with. There should be one Span corresponding to Record consumption
        //that "follows from" the Span that was active when we logged Records to Kafka. That Span
        //will contain log entries for all of the operations applied to the received Kafka Record
        //and its subsequent transformations, ultimately culminating in acknowledgement
        System.out.println("Number of Spans: " + sortedSpans.size());
        for (int i = 0; i < sortedSpans.size(); i++) {
            System.out.println(String.format("================ Span #%d ================", i + 1));
            System.out.println("Trace ID: " + sortedSpans.get(i).context().traceId());
            System.out.println("Span ID: " + sortedSpans.get(i).context().spanId());
            System.out.println("Parentage: " + extractParentage(sortedSpans.get(i)));
            System.out.println("Operation Name: " + sortedSpans.get(i).operationName());

            System.out.println("Number of Log Entries: " + sortedSpans.get(i).logEntries().size());
            for (int j = 0; j < sortedSpans.get(i).logEntries().size(); j++) {
                System.out.println(String.format("Log Entry #%d: timestamp=%d fields=%s",
                    j, sortedSpans.get(i).logEntries().get(j).timestampMicros(), sortedSpans.get(i).logEntries().get(j).fields()));
            }
        }

        System.exit(0);
    }

    private static String extractParentage(MockSpan mockSpan) {
        return mockSpan.references().isEmpty() ? "Root Span" :
            String.format("%s Span with ID %d", mockSpan.references().get(0).getReferenceType(), mockSpan.parentId());
    }
}
