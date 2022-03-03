# Axon Framework - Kafka Extension 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/org.axonframework.extensions.kafka/axon-kafka/badge.svg)](https://maven-badges.herokuapp.com/maven-central/org.axonframework.extensions.kafka/axon-kafka/)
![Build Status](https://github.com/AxonFramework/extension-kafka/workflows/Kafka%20Extension/badge.svg?branch=master)
[![SonarCloud Status](https://sonarcloud.io/api/project_badges/measure?project=AxonFramework_extension-kafka&metric=alert_status)](https://sonarcloud.io/dashboard?id=AxonFramework_extension-kafka)

Axon Framework is a framework for building evolutionary, event-driven microservice systems,
based on the principles of Domain-Driven Design, Command-Query Responsibility Separation (CQRS), and Event Sourcing.

As such, it provides the necessary building blocks to follow these principles.
Examples of these building blocks are Aggregate factories and Repositories, Command, Event and Query Buses, and an Event Store.
The framework provides sensible defaults for all of these components out of the box.

This setup helps to create a well-structured application without having to bother with the infrastructure.
The main focus can thus become the business functionality.

This repository provides an extension to the Axon Framework: [Kafka](https://kafka.apache.org/).
It provides functionality to leverage Kafka to send and receive Events from one (micro)service to another.
Thus, it does not include command or query distribution, nor event store specifics required for event sourcing.
  
For more information on anything Axon, please visit our website, [http://axoniq.io](http://axoniq.io).

## Getting started

The [reference guide](https://docs.axoniq.io) contains a separate chapter for all the extensions.
The Kafka extension description can be found [here](https://docs.axoniq.io/reference-guide/extensions/kafka).
This extension should be regarded as a partial replacement of [Axon Server](https://axoniq.io/product-overview/axon-server),
 since it only cover the event routing part.

## Receiving help

Are you having trouble using the extension? 
We'd like to help you out the best we can!
There are a couple of things to consider when you're traversing anything Axon:

* Checking the [reference guide](https://docs.axoniq.io/reference-guide/extensions/kafka) should be your first stop,
 as the majority of possible scenarios you might encounter when using Axon should be covered there.
* If the Reference Guide does not cover a specific topic you would've expected,
 we'd appreciate if you could file an [issue](https://github.com/AxonIQ/reference-guide/issues) about it for us. 
* There is a [forum](https://discuss.axoniq.io/) to support you in the case the reference guide did not sufficiently answer your question.
Axon Framework and Server developers will help out on a best effort basis.
Know that any support from contributors on posted question is very much appreciated on the forum.
* Next to the forum we also monitor Stack Overflow for any questions which are tagged with `axon`.

## Feature requests and issue reporting

We use GitHub's [issue tracking system](https://github.com/AxonFramework/extension-kafka/issues) for new feature 
request, extension enhancements and bugs. 
Prior to filing an issue, please verify that it's not already reported by someone else.

When filing bugs:
* A description of your setup and what's happening helps us figuring out what the issue might be
* Do not forget to provide version you're using
* If possible, share a stack trace, using the Markdown semantic ```

When filing features:
* A description of the envisioned addition or enhancement should be provided
* (Pseudo-)Code snippets showing what it might look like help us understand your suggestion better 
* If you have any thoughts on where to plug this into the framework, that would be very helpful too
* Lastly, we value contributions to the framework highly. So please provide a Pull Request as well!
 
