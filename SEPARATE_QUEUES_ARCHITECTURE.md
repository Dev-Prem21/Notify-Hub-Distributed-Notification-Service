# NotifyHub - Separate Queues + Dead-Letter Queue Implementation

## Architecture Overview

This implementation uses a **distributed notification system** with independent queues and automatic retry/DLQ handling.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                                                                                   │
│                           USER SERVICE (Port 8080)                               │
│                                    │                                             │
│                          Publishes user.events                                   │
│                                    │                                             │
│                                    ▼                                             │
│                  ┌─────────────────────────────────────┐                         │
│                  │  KAFKA BROKER (localhost:9092)      │                         │
│                  │  Topic: user.events                 │                         │
│                  └─────────────────────────────────────┘                         │
│                                    │                                             │
│                                    ▼                                             │
│        ┌───────────────────────────────────────────────────────────┐             │
│        │  NOTIFICATION SERVICE (Port 8090)                          │             │
│        │  UserEventConsumer (Router/Dispatcher)                     │             │
│        └───────────────┬───────────────┬───────────────┬────────────┘             │
│                        │               │               │                         │
│         ┌──────────────▼─┐  ┌─────────▼──────┐  ┌────▼──────────┐               │
│         │  email.events  │  │  sms.events    │  │ push.events   │               │
│         └────────┬────────┘  └────────┬───────┘  └────┬──────────┘               │
│                  │                    │               │                         │
│                  ▼                    ▼               ▼                         │
│         ┌─────────────────┐  ┌─────────────┐  ┌──────────────┐                 │
│         │ EmailConsumer   │  │ SmsConsumer │  │ PushConsumer │                 │
│         │ (Retries: 3x)   │  │ (Retries 3x)│  │ (Retries 3x) │                 │
│         └────────┬────────┘  └────────┬────┘  └────┬─────────┘                 │
│                  │                    │            │                           │
│         ┌────────▼──────────────────────────────────▼──────────────────┐        │
│         │  EXTERNAL SERVICES (Simulated in demo)                       │        │
│         │  - Email Service (10% failure rate)                          │        │
│         │  - SMS Service (15% failure rate)                            │        │
│         │  - Push Service (5% failure rate)                            │        │
│         └────────┬──────────────────────────────────────────────────────┘       │
│                  │                                                              │
│         ┌────────▼────────┬──────────────┬──────────────┐                       │
│         │                 │              │              │                       │
│         ▼                 ▼              ▼              ▼                       │
│    ✓ Success      email-dlt        sms-dlt        push-dlt                      │
│                   (Dead Letter)    (Dead Letter)  (Dead Letter)                 │
│                        │                │              │                       │
│                        └────────┬───────┴──────────────┘                       │
│                                 ▼                                              │
│                   DeadLetterQueueConsumer                                       │
│                   - Alerts ops team                                            │
│                   - Logs failure details                                       │
│                   - Stores for manual review                                   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Key Components

### 1. **User Event Consumer** (Router/Dispatcher)
- **Location**: `UserEventConsumer.java`
- **Purpose**: Routes user registration events to 3 separate notification channels
- **Topics Consumed**: `user.events`
- **Topics Produced**: 
  - `email.events`
  - `sms.events`
  - `push.events`
- **Behavior**: Publishes the same event to all 3 topics independently

```java
// Pseudo-code
When user.events arrives:
  ├─ Publish to email.events
  ├─ Publish to sms.events
  └─ Publish to push.events
```

### 2. **Independent Notification Consumers**

#### Email Notification Consumer
- **File**: `EmailNotificationConsumer.java`
- **Topic**: `email.events`
- **Consumer Group**: `email-notification-consumer-group`
- **Retry Configuration**: 3 attempts with exponential backoff (1s → 2s → 4s)
- **DLQ**: `email.events-dlt`

#### SMS Notification Consumer
- **File**: `SmsNotificationConsumer.java`
- **Topic**: `sms.events`
- **Consumer Group**: `sms-notification-consumer-group`
- **Retry Configuration**: 3 attempts with exponential backoff
- **DLQ**: `sms.events-dlt`

#### Push Notification Consumer
- **File**: `PushNotificationConsumer.java`
- **Topic**: `push.events`
- **Consumer Group**: `push-notification-consumer-group`
- **Retry Configuration**: 3 attempts with exponential backoff
- **DLQ**: `push.events-dlt`

### 3. **Notification Services** (Business Logic)

#### EmailNotificationService
- **File**: `EmailNotificationService.java`
- **Failure Rate**: 10% (for demo)
- **Simulates**: Email sending via SendGrid/AWS SES

#### SmsNotificationService
- **File**: `SmsNotificationService.java`
- **Failure Rate**: 15% (for demo)
- **Simulates**: SMS sending via Twilio/AWS SNS

#### PushNotificationService
- **File**: `PushNotificationService.java`
- **Failure Rate**: 5% (for demo)
- **Simulates**: Push notifications via Firebase/Apple Push

### 4. **Dead Letter Queue Consumer**
- **File**: `DeadLetterQueueConsumer.java`
- **Purpose**: Handles messages that failed after all retries
- **Listens To**:
  - `email.events-dlt`
  - `sms.events-dlt`
  - `push.events-dlt`
- **Actions**:
  - ✉️ Alerts ops team
  - 📝 Logs failure details
  - 🏪 Stores for manual review

## Message Flow Example

### Scenario: Alice registers as a user

```
1. Client sends POST /api/users to User Service
   {
     "id": "user123",
     "name": "Alice",
     "email": "alice@example.com"
   }

2. User Service publishes to user.events:
   {
     "userId": "user123",
     "name": "Alice",
     "email": "alice@example.com",
     "phoneNumber": "+1-555-0123",
     "eventType": "USER_REGISTERED"
   }

3. UserEventConsumer (Router) receives the event and publishes to:
   ├─→ email.events
   ├─→ sms.events
   └─→ push.events

4. Each consumer processes independently:
   
   EMAIL CONSUMER:
   ├─ Attempt 1: Try EmailService.sendEmail()
   │  └─ Fails (10% chance) → Wait 1s
   ├─ Attempt 2: Retry
   │  └─ Fails again → Wait 2s
   ├─ Attempt 3: Final retry
   │  └─ SUCCESS! ✓
   
   SMS CONSUMER:
   ├─ Attempt 1: Try SmsService.sendSms()
   │  └─ SUCCESS! ✓
   
   PUSH CONSUMER:
   ├─ Attempt 1: Try PushService.sendPush()
   │  └─ SUCCESS! ✓

5. If all retries fail for a service → Message sent to DLQ
   DeadLetterQueueConsumer:
   ├─ Logs: "[DLQ-EMAIL] Message failed after 3 retries"
   ├─ Alerts: Operations team via Slack/PagerDuty
   └─ Stores: In database for manual review

Result: Alice gets email (after retry), SMS, and push notification!
```

## Retry Logic Details

### @RetryableTopic Annotation

```java
@RetryableTopic(
    attempts = "3",                    // Retry 3 times (initial + 2 retries)
    delay = "1000",                    // Start with 1 second delay
    multiplier = "2.0",                // Double the delay each time (1s → 2s → 4s)
    autoCreateTopic = "true",          // Auto-create retry topics
    include = {CustomException.class},  // Which exceptions trigger retry
    dltStrategy = DltStrategy.FAIL_ON_ERROR  // Send to DLQ on final failure
)
@KafkaListener(topics = "email.events", groupId = "email-notification-consumer-group")
public void handleEmailNotification(...) { }
```

### Retry Timeline Example

If Email Service fails:

```
Time 0s   : Attempt 1 → FAIL
Time 1s   : Attempt 2 (after 1s delay) → FAIL
Time 3s   : Attempt 3 (after 2s delay) → FAIL or SUCCESS
Time 3s+  : If still failed → Send to email.events-dlt (DLQ)
```

## Error Handling Philosophy

### ✅ **Success Path**
```
Event arrives → Consumer processes → Service succeeds → Message acknowledged
```

### 🔄 **Retry Path**
```
Event arrives → Attempt 1 fails → Wait 1s → Attempt 2 → Wait 2s → Attempt 3
                                                            ↓
                                                    Success ✓ or Failure ✗
```

### ❌ **Failure Path**
```
All retries exhausted → Message sent to DLQ → DeadLetterQueueConsumer:
                                               ├─ Alert ops team
                                               ├─ Log details
                                               └─ Store for manual intervention
```

## Key Benefits

| Aspect | Before (Single Handler) | After (Separate Queues) |
|--------|------------------------|----------------------|
| **Isolation** | Email failure blocks SMS & Push | Each fails independently ✓ |
| **Scalability** | Single consumer | Each consumer scales separately ✓ |
| **Reliability** | One retry policy | Each has own retry strategy ✓ |
| **Observability** | Mixed logs | Separate logs per channel ✓ |
| **Failure Handling** | Lost messages | DLQ for investigation ✓ |
| **Recovery** | Manual restart | Automatic retries ✓ |

## Testing the Implementation

### Step 1: Build and Run
```bash
# Terminal 1: Start Kafka
cd "NotifyHub - Distributed Notification Service"
docker-compose up -d

# Terminal 2: User Service
cd user-service
mvn clean install
mvn spring-boot:run

# Terminal 3: Notification Service
cd notification-service
mvn clean install
mvn spring-boot:run
```

### Step 2: Register a User
```bash
curl -X POST http://localhost:8080/api/users \
  -H "Content-Type: application/json" \
  -d '{
    "id": "user123",
    "name": "Alice",
    "email": "alice@example.com"
  }'
```

### Step 3: Observe Console Output

**Notification Service Console:**
```
[USER-EVENT-ROUTER] Received user event from topic: user.events...
[USER-EVENT-ROUTER] ✓ Successfully routed event for user: user123 to all channels
[EMAIL CONSUMER] Received message from topic: email.events...
[EMAIL SERVICE] ✓ Email sent successfully to: alice@example.com
[SMS CONSUMER] Received message from topic: sms.events...
[SMS SERVICE] ✓ SMS sent successfully to: +1-555-0123
[PUSH CONSUMER] Received message from topic: push.events...
[PUSH SERVICE] ✓ Push notification sent successfully to user: user123
```

## Production Enhancements

### 1. Real Email Service
```java
@Service
public class EmailNotificationService {
    private final SendGridClient sendGridClient;
    
    public void sendEmail(NotificationEvent event) {
        sendGridClient.send(
            new Email()
                .setTo(event.getEmail())
                .setFrom("notifications@notifyhub.com")
                .setSubject("Welcome to NotifyHub, " + event.getName())
                .setBody("Thank you for registering...")
        );
    }
}
```

### 2. Real SMS Service
```java
@Service
public class SmsNotificationService {
    private final TwilioClient twilioClient;
    
    public void sendSms(NotificationEvent event) {
        twilioClient.messages().create(
            new CreateMessageRequest()
                .setTo(event.getPhoneNumber())
                .setFrom("+1234567890")
                .setBody("Welcome to NotifyHub!")
        );
    }
}
```

### 3. Monitoring & Alerting
```java
@Component
public class DeadLetterQueueConsumer {
    private final SlackClient slackClient;
    private final MetricsService metricsService;
    private final DatabaseService dbService;
    
    private void handleDlqMessage(...) {
        // Alert to Slack
        slackClient.sendAlert("Email notification failed for user: " + event.getUserId());
        
        // Record metrics
        metricsService.incrementCounter("dlq.email.failures");
        
        // Store for manual review
        dbService.saveFailedNotification(event);
        
        // Create ticket in Jira
        jiraClient.createIssue("Failed Notification", event);
    }
}
```

### 4. Persistence
```java
@Component
public class NotificationPersistenceService {
    private final NotificationRepository repo;
    
    public void recordNotificationAttempt(
        String userId, 
        NotificationType type, 
        NotificationStatus status) {
        
        Notification notification = new Notification();
        notification.setUserId(userId);
        notification.setType(type);
        notification.setStatus(status);
        notification.setTimestamp(LocalDateTime.now());
        
        repo.save(notification);
    }
}
```

## Troubleshooting

### Issue: Messages in DLQ
```
[DLQ-EMAIL] Message failed after all retries!
```

**Solution:**
1. Check service logs for error details
2. Verify external service (SendGrid, Twilio) is running
3. Check network connectivity
4. Implement manual retry mechanism in DLQ consumer

### Issue: Kafka topics not auto-creating
**Solution:** Ensure `auto.create.topics.enable: true` in `application.yml`

### Issue: Messages not being retried
**Solution:** Check `@RetryableTopic` annotation is present and correct exception type is thrown

## Summary

This implementation provides:
- ✅ **Independent notification channels** - no cascading failures
- ✅ **Automatic retry logic** - with exponential backoff
- ✅ **Dead-Letter Queues** - for failed messages
- ✅ **Better observability** - separate logs per channel
- ✅ **Scalability** - each consumer can be scaled independently
- ✅ **Production-ready** - hooks for monitoring, alerting, and persistence
