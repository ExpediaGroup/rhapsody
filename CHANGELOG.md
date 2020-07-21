# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
