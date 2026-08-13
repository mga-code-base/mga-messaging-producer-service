# 📤 mga-messaging-producer-service

[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.x-6DB33F?logo=springboot)](https://spring.io/)
[![Java Version](https://img.shields.io/badge/Java-17-007396?logo=openjdk)](https://openjdk.org/)
[![Build Tool](https://img.shields.io/badge/Build-Apache%20Maven%203.9.x-C71A36?logo=apachemaven)](https://maven.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Aiven%20Cloud-231F20?logo=apachekafka)](https://aiven.io/)

## 📌 Service Overview

`mga-messaging-producer-service` serves as the event ingestion hub for asynchronous domain events across the platform (such as class booking confirmations, member registration events, and broadcast announcements).

It receives event payloads from business services and publishes formatted JSON events directly to Apache Kafka hosted on Aiven Cloud.

### 🏛️ Architectural Features
* Event Ingestion Pipeline: Accepts asynchronous domain event payloads dispatched from internal microservices and serializes them into standardized JSON schemas.
* Apache Kafka Integration: Connects to Aiven Cloud Managed Kafka, publishing messages to dedicated event topics with partitioning and delivery guarantees.
* Standardized Data Contract: Imports domain models from `mga-all-models-jars` to ensure strict schema adherence across message producers and consumers.

---

## 🛠️ Technical Specifications
* Port: 8089 (Cluster-Internal Ingestion)
* Language/Build: Java 17 | Apache Maven 3.9.x
* Message Broker: Apache Kafka (Aiven Cloud Managed Kafka)
* Shared Contract: mga-all-models-jars

---

## ⚙️ Build & Deployment Instructions
# Standard JVM Build & Run
mvn clean package
java -jar target/mga-messaging-producer-service.jar

# Spring Boot Run
mvn spring-boot:run

```
