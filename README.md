<img src="assets/images/Rhapsody.png" width="25%">

# Rhapsody
[![Build Status](https://travis-ci.com/ExpediaGroup/rhapsody.svg?branch=master)](https://travis-ci.com/ExpediaGroup/rhapsody)

Rhapsody is an asynchronous message processing library that builds on the [Reactive Streams Specification](http://www.reactive-streams.org/) to deliver reusable functionalities under the following categories:
- At-Least-Once Processing Guarantee
- Quality of Service
- Observability

While delivering features that fall in to the above categories, Rhapsody aims to maintain the inherent developmental attributes afforded by the Reactive Streams Specification:
- Non-blocking backpressure
- Arbitrary parallelism
- Infrastructural Interoperability ([Kafka](https://kafka.apache.org/), [RabbitMQ](https://www.rabbitmq.com/), etc.)
- Implementation Interoperability ([ReactiveX/RxJava](https://github.com/ReactiveX/RxJava), [Project Reactor](https://projectreactor.io/), etc.)

Where appropriate, Rhapsody is backed by [Project Reactor](https://projectreactor.io/) to build Transformers and inject side-effect behaviors (like Logging and Metric collection).

## Background
Rhapsody started off as an [Inner Source](https://en.wikipedia.org/wiki/Inner_source) library at [Vrbo](https://www.vrbo.com/), an [Expedia Group](https://www.expediagroup.com/) brand. At time-of-inception, Vrbo was heavily expanding its usage of Kafka as the de facto asynchronous messaging system of choice.

The need for an alternative asynchronous streaming framework arose out of Vrbo's growing diversity of streaming use cases. Traditionally available streaming libraries, like [Kafka Streams](https://kafka.apache.org/documentation/streams/), were not quite flexible enough to support the superset of functionalites that developers were trying to build on top of Kafka. In particular, traditional streaming libraries make assumptions about what infrastructures are in play, what processing topologies should be available, and how easy it should be to design for failure. Vrbo's needs for asynchronous streaming use cases include(d) heterogeneous infrastructure endpoints (like RabbitMQ-to-Kafka), I/O-bound processing (message processing that requires API or Database interaction), [Extended Architecture (XA) / Two Phase Commit](https://dzone.com/articles/xa-transactions-2-phase-commit) Processing, Batch Processing, Message Deduplication, and others.

And so, Rhapsody was born to attempt providing a streaming framework that provides At-Least-Once Processing Guarantees (such as what we get from other traditional streaming libraries), arbitrary parallelism (to address I/O-bound processing scalability), and arbitrary interoperability with today's and the future's streaming infrastructures.

## Basics
Before getting started, it's important to note that Rhapsody does _not_ aim to be a replacement for any fluent Reactive Streams libraries. The goal of Rhapsody is to *build on existing Reactive Streams implementations* and provide functionality that primarily addresses usage with *infinite asynchronous flows of messages* while adhering to the Reactive Streams API. In particular, Rhapsody heavily integrates with [Project Reactor](https://projectreactor.io/) and related projects, like [Reactor Kafka](https://github.com/reactor/reactor-kafka) and [Reactor RabbitMQ](https://github.com/reactor/reactor-rabbitmq), to avoid re-implementation of existing Publisher and Subscriber implementations.

#### Project Reactor
We highly recommend getting familiar with Project Reactor via its [Learning Resources](https://projectreactor.io/learn) if you are not already familiar with Reactive Streams or any of its implementations.

#### At Least Once Processing
The key abstraction around which Rhapsody builds "At Least Once Processing Guarantees" is [Acknowledgeable](api/src/main/java/com/expedia/rhapsody/api/Acknowledgeable.java). The goal behind "acknowledgeability" is to restore a lightweight form of the bi-directional communication in control flow that we lose when moving from synchronous to asynchronous code. 

In synchronous control flows, we have the (dubious) benefit of being able to tightly couple the processing of any given input/message to its successful completion or abnormal termination (Errors/Exceptions). The same goes for asynchronous control flows where there is neither the presence of backpressure or thread boundaries. In either case, when the controlling thread completes the processing/emission of a unit of data without erroneous termination, there is a reasonable implication that the corresponding data has been successfully processed, or, at worst, any errors resulting from the processing of that data were gracefully handled. An opposite, mutually-exclusive implication is made when that processing results in an Error/Exception being raised/thrown.

In contrast, asynchronous control flows that may incorporate backpressure and/or arbitrary numbers of asynchronous boundaries do not typically have a semantic for communicating "successful completion" or "abnormal termination" to the emission sources of processed data. Acknowledgeability aims to address this by providing "channels" for "acknowledgement" and "nacknowledgement" (negative acknowledgement) that are logically coupled to the originating data emitted by a Publisher and propagated with that data's downstream transformations. For example, negatively acknowledging the processing/transformation of a Kafka Record allows us to emit the corresponding error (and hence not committing past its offset) and subsequently resubscribe such that the error-inducing Record is eventually reprocessed.

#### Quality of Service
Rhapsody has incrementally evolved to include commonly desired Quality of Service functionalities, like [Rate Limiting](core/src/main/java/com/expedia/rhapsody/core/transformer/RateLimitingTransformer.java), [Deduplication](core/src/main/java/com/expedia/rhapsody/core/transformer/DeduplicatingTransformer.java), and [Maximum In-Flight Acknowledgeability](reactor-kafka/src/main/java/com/expedia/rhapsody/kafka/acknowledgement/ReceiverAcknowledgementStrategy.java#L32). Like most of the features provided by Rhapsody, these are implemented as Publisher Transformers.

#### Observability
Rhapsody aims to provide observability in to Reactive Streams by leveraging existing Project Reactor integrations and integrating with standard observability APIs, like [SLF4J](https://www.slf4j.org/) for logging, [Micrometer](http://micrometer.io/) for metrics, and [OpenTracing](https://opentracing.io/)

## Getting Started
Rhapsody is a Java library that requires at least JDK 1.8+ for building and integration.

#### Building
Rhapsody is built using maven

```$bash
mvn clean verify
```

#### Usage
Check out the [Samples](samples) to see Rhapsody in action

## Contributing
Please refer to [CONTRIBUTING](CONTRIBUTING.md) for information on how to contribute to Rhapsody
