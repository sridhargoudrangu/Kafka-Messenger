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

### Installation
The Confluent Platform only provides services for Linux distributions such as Ubuntu and Bash on Ubuntu on Windows. 

Windows Only Installation (if necessary) for Bash on Ubuntu on Windows:
```sh
lxrun /install /y
```
Install Confluent's public key, which is used to sign the packages in the apt repository:

```sh
wget -qO - http://packages.confluent.io/deb/3.3/archive.key | sudo apt-key add -
```
Add the repository to your /etc/apt/sources.list:

```sh
sudo add-apt-repository "deb [arch=amd64] http://packages.confluent.io/deb/3.3 stable main"
```

Run apt-get update and install Confluent Open Source:

```sh
sudo apt-get update && sudo apt-get install confluent-platform-oss-2.11
```
To start the Confluent Platform (which consists of Zookeeper, Kafka, and Schema Registry) run:
```sh
confluent start kafka-rest
```
Each of the services will then attempt to start and print a status message as follows. 
```sh
Starting zookeeper
zookeeper is [UP]
Starting kafka
kafka is [UP]
Starting schema-registry
schema-registry is [UP]
```

If all services are "UP", the Commvault Kafka Messenger may be run.
