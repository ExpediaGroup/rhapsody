# Rhapsody Samples
This module contains Rhapsody streaming examples. Each example class is a runnable streaming sample.

## End to End to End
The following Kafka [End to End to End](src/main/java/com/expedia/rhapsody/samples/endtoendtoend) sequence shows the incremental steps necessary to reactively produce messages to a Kafka Cluster and apply downstream streaming operations
- [Kafka Part 1](src/main/java/com/expedia/rhapsody/samples/endtoendtoend/KafkaPart1.java): Use a Kafka Sender Factory to produce records to an embedded Kafka Broker
- [Kafka Part 2](src/main/java/com/expedia/rhapsody/samples/endtoendtoend/KafkaPart2.java): Consume sent records using Kafka Flux Factory
- [Kafka Part 3](src/main/java/com/expedia/rhapsody/samples/endtoendtoend/KafkaPart3.java): Extend Record consumption to stream process with at-least-once processing guarantee
- [Kafka Part 4](src/main/java/com/expedia/rhapsody/samples/endtoendtoend/KafkaPart4.java): Add another downstream consumer of processed results

## Infrastructural Interoperation
[RabbitmqToKafka](src/main/java/com/expedia/rhapsody/samples/infrastructuralinteroperability/RabbitmqToKafka.java) shows how Rhapsody allows interoperability between RabbitMQ (as a source/Publisher) and Kafka (as a sink/Subscriber) while maintaining at-least-once processing guarantees

## Parallelism
[Kafka Topic Partition Parallelism](src/main/java/com/expedia/rhapsody/samples/parallelism/KafkaTopicPartitionParallelism.java) shows how to parallelize processing of Kafka Records and subsequent transformations by grouping of Topic-Partitions and assigning a Thread per group.
 
[Kafka Arbitrary Parallelism](src/main/java/com/expedia/rhapsody/samples/parallelism/KafkaArbitraryParallelism.java) shows how to parallelize processing of Kafka Records and subsequent transformations by applying arbitrary grouping and assigning a Thread per group. This example (as well as the previous one) highly leverage built-in [Acknowledgement Queueing](../core/src/main/java/com/expedia/rhapsody/core/acknowledgement/AcknowledgementQueuingSubscriber.java) to guarantee in-order acknowledgement of Record offsets.

## Error Handling
[Kafka Error Handling](src/main/java/com/expedia/rhapsody/samples/errorhandling/KafkaErrorHandling.java) shows how to apply resiliency to the processing of Kafka Records, both to possible upstream errors and downstream acknowledgement errors.

## Deduplication
[Kafka Deduplication](src/main/java/com/expedia/rhapsody/samples/deduplication/KafkaDeduplication.java) shows how to add message deduplication to the processing of a Kafka topic. This example maintains the incorporation of [Acknowledgement](../api/src/main/java/com/expedia/rhapsody/api/Acknowledgeable.java) propagation such as to maintain at-least-once processing guarantees

## OpenTracing
[Kafka OpenTracing](src/main/java/com/expedia/rhapsody/samples/opentracing/KafkaOpenTracing.java) shows how we can decorate the production and consumption of Kafka Records with OpenTracing context, as well as add logging/eventing to those contexts for notable transformations of Kafka Records

## Metrics
[Kafka Metrics](src/main/java/com/expedia/rhapsody/samples/metrics/KafkaMetrics.java) shows how Rhapsody integrates with Micrometer to provide Metrics from native Kafka Reporting and from available Metric transformation in streaming processes
