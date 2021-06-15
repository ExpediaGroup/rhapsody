# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.12]

rhapsody `0.5.12` is a beta release containing updates outlined below

### Additions

* There is now a JMX-manageable StanzaBundle for Dropwizard

## [0.5.11]

rhapsody `0.5.11` is a beta release containing updates outlined below

### Additions

* Running state of Stanza is now query-able via `Stanza::state`

## [0.5.10]

rhapsody `0.5.10` is a beta release containing updates outlined below.

### Changes

* Builds and Releases are now conducted via GitHub Actions

## [0.5.8 - 0.5.9]

rhapsody `0.5.8` - `0.5.9` are beta releases containing updates outlined below.

### Fixes

* ResubscribingTransformer no longer uses deprecated `retryWhen`
* SenderFactories no longer use deprecated `retry`

## [0.5.7]

rhapsody `0.5.7` is a beta release containing updates outlines below

### Fixes

* Addressed bug where Work Buffering and Deduplication can drop items under high load. See relevant [Reactor issue](https://github.com/reactor/reactor-core/issues/2352)

## [0.5.6]

rhapsody `0.5.6` is a beta release containing updates outlines below

### Additions

* Source prefetch is now configurable on Work Buffering and Deduplication Transformers

## [0.5.5]

rhapsody `0.5.5` is a beta release containing updates outlines below

### Additions

* SenderFactories now have more convenient functional Sender methods

## [0.5.4]

rhapsody `0.5.4` is a beta release containing updates outlines below

### Additions

* Bounded Elastic Scheduler is now configurable in SchedulerType

### Fixes

* AbstractAllOrNothingPartitionAssignor fixed to be JRE backward compatible on ByteBuffer::flip

## [0.5.3]

rhapsody `0.5.3` is a beta release containing updates outlines below

### Fixes

* Javadoc update

## [0.5.2]

rhapsody `0.5.2` is a beta release containing updates outlines below

### Additions

* TracingAcknowledgeable activates Span for availability in `publish` function

## [0.5.1]

rhapsody `0.5.1` is a beta release containing updates outlines below

### Fixes

* dropwizard-metrics version fixed to reference 4.1.6 vs. 4.1.9
* jersey-bom fixed to reference 2.30.1 vs. 2.31

## [0.5.0]

rhapsody `0.5.0` is a beta release containing updates outlines below

### Changes

* Dropwizard 1.1.8 has been upgraded to 2.0.8
* Kafka has been upgraded to 2.3.1
* Confluent has been upgraded to 5.2.4
* Jakarta dependencies are now used (where explicit) instead of javax dependencies

## [0.4.1]

rhapsody `0.4.1` is a beta release containing updates outlined below.

### Changes

* Project can now be built with JDK11 (and is done so on Travis)
* OpenTracing API has been upgraded to 0.32.0

### Fixes

* OpenTracing code uses non-deprecated methods from 0.32.0

## [0.3.6 - 0.3.7]

rhapsody `0.3.6` - `0.3.7` are beta releases containing updates outlined below.

### Fixes

* Addressed bug where Work Buffering and Deduplication can drop items under high load. See relevant [Reactor issue](https://github.com/reactor/reactor-core/issues/2352)
* ResubscribingTransformer no longer uses deprecated `retryWhen`

## [0.3.5]

rhapsody `0.3.5` is a beta release containing updates outlined below.

### Additions

* TracingAcknowledgeable activates Span for availability in `publish` function
* SenderFactories now have more convenient functional Sender methods
* Source prefetch is now configurable on Work Buffering and Deduplication Transformers

### Fixes

* Javadoc update

## [0.3.4]

rhapsody `0.3.4` is a beta release containing updates outlined below.

### Additions

* Bounded Elastic scheduler can now be configured via Scheduler Type

### Fixes

* AbstractAllOrNothingPartitionAssignor fixed to be JRE backward compatible on ByteBuffer::flip

## [0.3.1 - 0.3.3]

rhapsody `0.3.1`-`0.3.3` are beta releases containing updates outlined below.

### Fixes

* Travis CI now builds with Java11
* Javadoc generation fixed to work with Java11
* Module structure refactored to use parent-pom pattern

## [0.3.0]

rhapsody `0.3.0` is a beta release containing updates outlined below.

### Additions

* Implementation of [Non-blocking Work preparation](../../issues/56)

### Fixes

* Tracing name of Throwing Consumption has been [fixed](../../pull/54)

## [0.2.1]

rhapsody `0.2.1` is a beta release containing updates outlined below.

### Additions

* Acknowledgeable now supports [consuming with Consumers that throw](../../pull/52)

## [0.2.0]

rhapsody `0.2.0` is a beta release containing updates outlined below.

### Additions

* [Stanza](core/src/main/java/com/expediagroup/rhapsody/core/stanza/Stanza.java) Framework
* Dropwizard Module
* [Dropwizard Stanza Bundle](dropwizard/src/main/java/com/expediagroup/rhapsody/dropwizard/stanza/StanzaBundle.java)

### Changes

* Dropwizard-specific code moved to Dropwizard module
