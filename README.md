# Commvault Kafka Messenger
The Commvault Kafka Messenger is a data backup and data streaming tool in Java which allows users to quickly post structured data to the Commvault Web Analytics Engine, a package software package that performs metadata indexing, content indexing, and other data collection operations for a variety of Commvault products and features.

The Commvault Kafka Messenger utilizes Confluent Platform, a plaform which provides the organization, transport, and tools necessary to coordinate between multiple data sources, applications, and sinks. The Commvault Kafka Messenger utilizes the Confluent Platform's Kafka REST Proxy, which offers a RESTful interface for Kafka clusters, to abstract the native Kafka Producer client, making it easy to public, process, and safely store data streams within partitioned Kafka clusters.

The Commvault Kafka Messenger also dynamically creates multiple Kafka Consumer clients to consume data in parallel from a partitioned Kafka cluster and generates multiple POST requests to send data to the Commvault Web Analytics Engine.

## Getting Started

### Requirements
The Commvault Kafka Messenger and Confluent Plaform require a 1.7 or later version of Oracle Java. Check the JRE installation requirements for your plaform if you intend to install Java. Prior to installing the Confluent Platform, double check your Java version:

```sh
java -version
```
The Commvault Kafka Messenger requires and already configured data source through the Commvault Web Analytics Engine and supporting software from Commvault.
