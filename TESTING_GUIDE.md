# Testing Guide: Separate Queues + Dead-Letter Queue Architecture

## Complete Testing Walkthrough

### Prerequisites
- Java 17+
- Maven 3.6+
- Docker & Docker Compose
- curl or Postman

---

## Step 1: Start All Services

### Terminal 1: Start Kafka & Zookeeper
```bash
cd "NotifyHub - Distributed Notification Service"
docker-compose up -d

# Wait 10-15 seconds for services to be ready
docker-compose ps
```

Expected output:
```
NAME           COMMAND                  SERVICE      STATUS
kafka          ...                      kafka        Up (healthy)
zookeeper      ...                      zookeeper    Up (healthy)
```

### Terminal 2: Start User Service
```bash
cd user-service
mvn clean install
mvn spring-boot:run
```

Expected output:
```
Started UserServiceApplication in 5.234 seconds (JVM running for 5.678)
Tomcat started on port(s): 8080
```

### Terminal 3: Start Notification Service
```bash
cd notification-service
mvn clean install
mvn spring-boot:run
```

Expected output:
```
Started NotificationServiceApplication in 6.123 seconds (JVM running for 6.456)
Tomcat started on port(s): 8090
```

---

## Step 2: Register a User

### Using curl
```bash
curl -X POST http://localhost:8080/api/users \
  -H "Content-Type: application/json" \
  -d '{
    "id": "user001",
    "name": "John Doe",
    "email": "john@example.com"
  }' \
  -v
```

### Using Postman
1. Open Postman
2. Create new POST request
3. URL: `http://localhost:8080/api/users`
4. Headers: `Content-Type: application/json`
5. Body (raw JSON):
```json
{
  "id": "user001",
  "name": "John Doe",
  "email": "john@example.com"
}
```
6. Click Send

### Expected Response
```
HTTP/1.1 201 Created
Content-Type: application/json

{
  "id": "user001",
  "name": "John Doe",
  "email": "john@example.com"
}
```

---

## Step 3: Observe Console Output

### Expected Notification Service Console Output

```
2024-07-01 10:15:23 [USER-EVENT-ROUTER] Received user event from topic: user.events, 
partition: 0, offset: 0 | Event: {...}

2024-07-01 10:15:23 [ROUTER] ✓ Routed event to email.events for user: user001

2024-07-01 10:15:23 [ROUTER] ✓ Routed event to sms.events for user: user001

2024-07-01 10:15:23 [ROUTER] ✓ Routed event to push.events for user: user001

2024-07-01 10:15:23 [USER-EVENT-ROUTER] ✓ Successfully routed event for user: user001 
to all notification channels

2024-07-01 10:15:23 [EMAIL CONSUMER] Received message from topic: email.events, 
partition: 0, offset: 0 | Event: {...}

2024-07-01 10:15:23 [EMAIL SERVICE] Processing email for user: user001 (john@example.com)

2024-07-01 10:15:23 [EMAIL SERVICE] ✓ Email sent successfully to: john@example.com

2024-07-01 10:15:23 [EMAIL CONSUMER] ✓ Successfully processed email notification 
for user: user001

2024-07-01 10:15:23 [SMS CONSUMER] Received message from topic: sms.events, 
partition: 0, offset: 0 | Event: {...}

2024-07-01 10:15:23 [SMS SERVICE] Processing SMS for user: user001 (+1-000-0000)

2024-07-01 10:15:23 [SMS SERVICE] ✓ SMS sent successfully to: +1-000-0000

2024-07-01 10:15:23 [SMS CONSUMER] ✓ Successfully processed SMS notification 
for user: user001

2024-07-01 10:15:23 [PUSH CONSUMER] Received message from topic: push.events, 
partition: 0, offset: 0 | Event: {...}

2024-07-01 10:15:23 [PUSH SERVICE] Processing push notification for user: user001

2024-07-01 10:15:23 [PUSH SERVICE] ✓ Push notification sent successfully to user: user001

2024-07-01 10:15:23 [PUSH CONSUMER] ✓ Successfully processed push notification 
for user: user001
```

---

## Step 4: Test Failure Scenarios

### Scenario A: Trigger Multiple Registrations to See Retries

Run these commands in quick succession:

```bash
# User 1
curl -X POST http://localhost:8080/api/users \
  -H "Content-Type: application/json" \
  -d '{"id": "user002", "name": "Alice", "email": "alice@example.com"}'

# User 2
curl -X POST http://localhost:8080/api/users \
  -H "Content-Type: application/json" \
  -d '{"id": "user003", "name": "Bob", "email": "bob@example.com"}'

# User 3
curl -X POST http://localhost:8080/api/users \
  -H "Content-Type: application/json" \
  -d '{"id": "user004", "name": "Charlie", "email": "charlie@example.com"}'
```

Since email has a 10% failure rate, some will succeed immediately, some might fail and retry.

**Observe**: Look at the notification service logs for:
- ✓ Successful sends
- 🔄 Retries (with timestamps showing the 1s, 2s delays)

### Scenario B: Watch for Email Retry

Continue sending requests until you see an email failure and retry:

```bash
for i in {1..20}; do
  curl -X POST http://localhost:8080/api/users \
    -H "Content-Type: application/json" \
    -d "{\"id\": \"retry_user_$i\", \"name\": \"User $i\", \"email\": \"user$i@example.com\"}" \
    2>/dev/null
  sleep 0.5
done
```

Expected retry log output:
```
2024-07-01 10:16:05 [EMAIL CONSUMER] Received message from topic: email.events...

2024-07-01 10:16:05 [EMAIL SERVICE] Processing email for user: retry_user_7...

2024-07-01 10:16:05 [EMAIL SERVICE] ✗ Email failed: Email server temporarily unavailable

2024-07-01 10:16:05 [EMAIL CONSUMER] ✗ Failed to send email for user: retry_user_7. Will retry.

2024-07-01 10:16:06 [EMAIL CONSUMER] Received message from topic: email.events-retry...

2024-07-01 10:16:06 [EMAIL SERVICE] Processing email for user: retry_user_7...

2024-07-01 10:16:06 [EMAIL SERVICE] ✓ Email sent successfully to: user7@example.com

2024-07-01 10:16:06 [EMAIL CONSUMER] ✓ Successfully processed email notification for user: retry_user_7
```

Notice:
- First attempt failed at 10:16:05
- Retry automatically happened at 10:16:06 (1 second later)
- Second attempt succeeded!

---

## Step 5: View Kafka Topics

### List all topics created
```bash
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092
```

Expected output:
```
__consumer_offsets
__transaction_state
email.events
email.events-dlt
email.events-retry
push.events
push.events-dlt
push.events-retry
sms.events
sms.events-dlt
sms.events-retry
user.events
```

### View messages in a topic
```bash
# View email.events topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic email.events \
  --from-beginning \
  --max-messages 5

# View DLQ
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic email.events-dlt \
  --from-beginning \
  --max-messages 5
```

---

## Step 6: Check Consumer Lag

View how far behind each consumer is:

```bash
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group email-notification-consumer-group \
  --describe
```

Expected output:
```
GROUP                              TOPIC          PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
email-notification-consumer-group  email.events   0          10              10              0
```

- **CURRENT-OFFSET**: Messages processed so far
- **LOG-END-OFFSET**: Total messages in topic
- **LAG**: 0 means caught up!

---

## Step 7: Test All Three Scenarios

### Test Case 1: All Services Succeed
```bash
curl -X POST http://localhost:8080/api/users \
  -H "Content-Type: application/json" \
  -d '{"id": "success_user", "name": "Lucky", "email": "lucky@example.com"}'
```

Expected: All three notifications succeed immediately.

### Test Case 2: One Service Fails Then Succeeds
Run the request 10+ times and watch for one that fails and retries:

```bash
for i in {1..10}; do
  curl -X POST http://localhost:8080/api/users \
    -H "Content-Type: application/json" \
    -d "{\"id\": \"test_user_$i\", \"name\": \"Test User $i\", \"email\": \"test$i@example.com\"}" \
    2>/dev/null
  sleep 1
done
```

Watch for SMS failures (15% failure rate - higher than email):
- Attempt 1: Fails
- Attempt 2 (after 1s): May fail again
- Attempt 3 (after 2s): Usually succeeds

### Test Case 3: Force DLQ by Stopping Email Service

Since we're simulating the service, we can't easily stop it. But with the 15% SMS failure rate, some messages might reach the DLQ after 3 failed retries.

If you see in logs:
```
[DLQ-SMS] Message failed after all retries!
```

This means the message is in the `sms.events-dlt` topic!

---

## Monitoring Dashboard

Create a simple monitoring view:

### Terminal 4: Monitor Topic Sizes
```bash
watch -n 1 'docker exec kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --describe | grep -E "^Topic|email|sms|push"'
```

This shows topic details every second.

### Terminal 5: Monitor DLQ Messages
```bash
watch -n 2 'docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic email.events-dlt \
  --from-beginning \
  --max-messages 1 \
  2>/dev/null || echo "No DLQ messages"'
```

This shows if messages are being sent to the dead-letter queue.

---

## Performance Testing

### Load Test: Send 100 User Registrations

```bash
#!/bin/bash

echo "Sending 100 user registrations..."
for i in {1..100}; do
  curl -X POST http://localhost:8080/api/users \
    -H "Content-Type: application/json" \
    -d "{\"id\": \"load_test_user_$i\", \"name\": \"Load Test User $i\", \"email\": \"load$i@example.com\"}" \
    -s > /dev/null
  
  if [ $((i % 10)) -eq 0 ]; then
    echo "  Sent $i requests..."
  fi
done

echo "All 100 requests sent!"
echo "Check notification service logs to see processing..."
```

Then check:
1. How long it takes to process all notifications
2. How many succeeded vs retried
3. How many are in DLQ (if any failed completely)

---

## Cleanup

### Stop All Services
```bash
# Terminal 1: Stop Kafka
docker-compose down -v

# Terminal 2 & 3: Press Ctrl+C to stop services
```

### Remove Docker Volumes (clean slate)
```bash
docker volume prune -f
```

---

## Expected Behavior Summary

| Event | Expected Output | Timeout |
|-------|-----------------|---------|
| User registration | 201 Created | <1s |
| Router publishes to 3 topics | Log: "routed to all channels" | <1s |
| Email succeeds | "✓ Email sent successfully" | <1s (avg) |
| SMS succeeds | "✓ SMS sent successfully" | <1s (avg) |
| Push succeeds | "✓ Push sent successfully" | <1s (avg) |
| Email fails then retries | "Will retry" → "✓ Email sent" | ~2-3s |
| SMS fails and retries | "Will retry" → "✓ SMS sent" | ~2-3s |
| All retries fail | Message in DLQ, DLQ log message | ~3s |

---

## Troubleshooting

### No logs appearing
- Check that notification service is running on port 8090
- Check `application.yml` has correct logging level (DEBUG)

### Messages not routing
- Verify UserEventConsumer is listening
- Check Kafka broker is running: `docker-compose ps`
- Check bootstrap server in code matches docker-compose (localhost:9092)

### DLQ not being used
- DLQ only fills when all 3 retries fail
- With small failure rates (5-15%), DLQ might be empty
- Create many requests (100+) to see DLQ usage

### Messages stuck in retry topic
- Check if consumer is running
- Verify consumer group exists
- Check for exceptions in logs

---

## Next Steps

1. **Monitor Success Rate**: Track % of notifications succeeding on first attempt
2. **Track Retry Rate**: Identify which services need more reliable infrastructure
3. **Analyze DLQ**: If DLQ fills up, investigate root cause
4. **Set Alerts**: Configure alerts when DLQ size exceeds threshold
5. **Add Persistence**: Store notification delivery status in database
