# Rhapsody Samples
This module contains Rhapsody streaming examples. Each example class is a runnable streaming sample.

## End to End to End
The following Kafka [End to End to End](core/src/main/java/com/expediagroup/rhapsody/samples/endtoendtoend) sequence shows the incremental steps necessary to reactively produce messages to a Kafka Cluster and apply downstream streaming operations
- [Kafka Part 1](core/src/main/java/com/expediagroup/rhapsody/samples/endtoendtoend/KafkaPart1.java): Use a Kafka Sender Factory to produce records to an embedded Kafka Broker
- [Kafka Part 2](core/src/main/java/com/expediagroup/rhapsody/samples/endtoendtoend/KafkaPart2.java): Consume sent records using Kafka Flux Factory
- [Kafka Part 3](core/src/main/java/com/expediagroup/rhapsody/samples/endtoendtoend/KafkaPart3.java): Extend Record consumption to stream process with at-least-once processing
- [Kafka Part 4](core/src/main/java/com/expediagroup/rhapsody/samples/endtoendtoend/KafkaPart4.java): Add another downstream consumer of processed results

## Infrastructural Interoperation
[RabbitmqToKafka](core/src/main/java/com/expediagroup/rhapsody/samples/infrastructuralinteroperability/RabbitmqToKafka.java) shows how Rhapsody allows interoperability between RabbitMQ (as a source/Publisher) and Kafka (as a sink/Subscriber) while maintaining at-least-once processing guarantee. For completeness, [KafkaToRabbitMQ](core/src/main/java/com/expediagroup/rhapsody/samples/infrastructuralinteroperability/KafkaToRabbitmq.java) shows the inverse, still maintaining at-least-once guarantees.

## Parallelism
[Kafka Topic Partition Parallelism](core/src/main/java/com/expediagroup/rhapsody/samples/parallelism/KafkaTopicPartitionParallelism.java) shows how to parallelize processing of Kafka Records and subsequent transformations by grouping of Topic-Partitions and assigning a Thread per group.
 
[Kafka Arbitrary Parallelism](core/src/main/java/com/expediagroup/rhapsody/samples/parallelism/KafkaArbitraryParallelism.java) shows how to parallelize processing of Kafka Records and subsequent transformations by applying arbitrary grouping and assigning a Thread per group. This example (as well as the previous one) highly leverage built-in [Acknowledgement Queueing](../core/src/main/java/com/expediagroup/rhapsody/core/acknowledgement/AcknowledgementQueuingSubscriber.java) to guarantee in-order acknowledgement of Record offsets.

## Error Handling
[Kafka Error Handling](core/src/main/java/com/expediagroup/rhapsody/samples/errorhandling/KafkaErrorHandling.java) shows how to apply resiliency to the processing of Kafka Records, both to possible upstream errors and downstream Acknowledgement Errors.

## Deduplication
[Kafka Deduplication](core/src/main/java/com/expediagroup/rhapsody/samples/deduplication/KafkaDeduplication.java) shows how to add deduplication to the processing of a Kafka topic. This example maintains the incorporation of [Acknowledgement](../api/src/main/java/com/expediagroup/rhapsody/api/Acknowledgeable.java) propagation such as to maintain at-least-once processing guarantee

## OpenTracing
[Kafka OpenTracing](core/src/main/java/com/expediagroup/rhapsody/samples/opentracing/KafkaOpenTracing.java) shows how we can decorate the production and consumption of Kafka Records with OpenTracing context, as well as add logging/eventing to those contexts for notable transformations of Kafka Records

## Metrics
[Kafka Metrics](core/src/main/java/com/expediagroup/rhapsody/samples/metrics/KafkaMetrics.java) shows how Rhapsody integrates with Micrometer to provide Metrics from native Kafka Reporting and from available Metric transformation in streaming processes

## Dropwizard
[Sample Kafka Application](dropwizard/src/main/java/com/exepdiagroup/rhapsody/samples/dropwizard/kafka/SampleKafkaApplication.java) demonstrates general intended usage of Rhapsody in Dropwizard applications