Two Spring Boot microservices with Kafka

This workspace contains two minimal Spring Boot microservices demonstrating asynchronous communication via Apache Kafka.

Services
- user-service (port 8080): exposes POST /api/users to register a user and publishes a "user-registered" event to topic `user.events`.
- notification-service (port 8090): exposes POST /api/events to publish application events and contains a Kafka consumer subscribed to `user.events` to simulate notification delivery.

Run
1. Start Kafka and Zookeeper:
   docker-compose up -d

2. Build and run services (from project root):
   cd "user-service" && mvn spring-boot:run
   cd "notification-service" && mvn spring-boot:run

Endpoints
- POST http://localhost:8080/api/users
  Body: {"id":"1","name":"Alice","email":"alice@example.com"}

- POST http://localhost:8090/api/events
  Body: {"type":"CUSTOM","payload":{}}

Notes
- This is a minimal example for development. Add proper validation, error handling, retries, schema registry, and secure Kafka for production.
