# Notify-Hub-Distributed-Notification-Service

A production-grade event-driven microservices system demonstrating enterprise-level asynchronous notification processing with Apache Kafka, Spring Boot, and resilience patterns.

This workspace contains two minimal Spring Boot microservices demonstrating asynchronous communication via Apache Kafka.

## Services
- **user-service** (port 8080): exposes `POST /api/users` to register a user and publishes a "user-registered" event to topic `user.events`.
- **notification-service** (port 8090): exposes `POST /api/events` to publish application events and contains a Kafka consumer subscribed to `user.events` to simulate notification delivery.

## Run
1. Start Kafka and Zookeeper: