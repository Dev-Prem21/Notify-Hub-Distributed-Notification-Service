# Architecture Diagrams - Separate Queues + Dead-Letter Queue

## Overall System Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                    CLIENT APPLICATION                                           │
│                                                                                                   │
│                              POST /api/users (Register User)                                    │
└───────────────────────────────────────┬─────────────────────────────────────────────────────────┘
                                        │
                                        ▼
                      ┌─────────────────────────────────┐
                      │    USER SERVICE (Port 8080)     │
                      │                                 │
                      │  UserController                 │
                      │  ├─ @PostMapping /api/users     │
                      │  └─ register(User user)         │
                      │      │                          │
                      │      └─ UserEventProducer       │
                      │         Publishes to:           │
                      │         user.events             │
                      └────────────┬────────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────────┐
                    │  KAFKA MESSAGE BROKER            │
                    │  Bootstrap: localhost:9092       │
                    │                                  │
                    │  Topic: user.events              │
                    │  Partitions: 1                   │
                    │  Replication: 1                  │
                    └────────────┬─────────────────────┘
                                 │
                                 ▼
            ┌────────────────────────────────────────────────────┐
            │ NOTIFICATION SERVICE (Port 8090)                   │
            │                                                    │
            │ ┌─────────────────────────────────────────────┐   │
            │ │  UserEventConsumer (Router/Dispatcher)      │   │
            │ │  @KafkaListener(topics="user.events")       │   │
            │ │                                              │   │
            │ │  Publishes same event to 3 topics:          │   │
            │ │  ├─ email.events                            │   │
            │ │  ├─ sms.events                              │   │
            │ │  └─ push.events                             │   │
            │ └────────────┬────────────┬────────────┬──────┘   │
            │              │            │            │           │
            │    ┌─────────▼─┐  ┌──────▼──┐  ┌─────▼──────┐    │
            │    │email.     │  │sms.     │  │push.       │    │
            │    │events     │  │events   │  │events      │    │
            │    │(Topic)    │  │(Topic)  │  │(Topic)     │    │
            │    └─────┬─────┘  └────┬────┘  └────┬───────┘    │
            │          │             │            │            │
            │  ┌───────▼──────┐  ┌──▼────────┐  ┌▼──────────┐ │
            │  │Email Notif.  │  │SMS Notif. │  │Push Notif.│ │
            │  │Consumer       │  │Consumer   │  │Consumer   │ │
            │  │               │  │           │  │           │ │
            │  │@RetryableTopic│  │@Retryable │  │@Retryable │ │
            │  │3 attempts     │  │3 attempts │  │3 attempts │ │
            │  │Backoff: exp.  │  │Backoff:ex │  │Backoff:ex │ │
            │  └───────┬──────┘  └──┬────────┘  └┬──────────┘ │
            │          │            │            │            │
            │  ┌───────▼──────┐  ┌──▼────────┐  ┌▼──────────┐ │
            │  │Email Service │  │SMS Service│  │Push Service
            │  │Failure: 10%  │  │Failure:15%│  │Failure: 5%│ │
            │  └───────┬──────┘  └──┬────────┘  └┬──────────┘ │
            │          │            │            │            │
            └──────────┼────────────┼────────────┼────────────┘
                       │            │            │
         ┌─────────────▼┐  ┌───────▼──┐  ┌────▼────────┐
         │Success ✓     │  │Success ✓ │  │Success ✓    │
         │              │  │           │  │             │
         │Message Acked │  │Message    │  │Message Acked│
         │              │  │Acked      │  │             │
         └──────────────┘  └───────────┘  └─────────────┘

                    ┌─────────────────────────────┐
                    │   Failed Messages Path       │
                    │   (After 3 retries fail)    │
                    └──────────────┬──────────────┘
                                   │
                   ┌───────────────┼───────────────┐
                   │               │               │
                   ▼               ▼               ▼
            ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
            │email.events-│  │sms.events- │  │push.events- │
            │dlt (DLQ)    │  │dlt (DLQ)   │  │dlt (DLQ)    │
            │             │  │            │  │             │
            │Failed msgs  │  │Failed msgs │  │Failed msgs  │
            │stored here  │  │stored here │  │stored here  │
            └──────┬──────┘  └──────┬─────┘  └──────┬──────┘
                   │                │               │
                   └────────────────┼───────────────┘
                                    │
                                    ▼
                   ┌────────────────────────────┐
                   │DeadLetterQueueConsumer     │
                   │                            │
                   │├─ Alert Ops Team 🚨        │
                   │├─ Log Failure Details 📝   │
                   │├─ Store in DB for Review   │
                   │└─ Create Incident Ticket   │
                   └────────────────────────────┘
```

## Message Flow: Happy Path (All Succeed)

```
Time 0s

┌─────────────────────────────────────────────────────────────────┐
│ Client: POST /api/users                                         │
│ Body: { "id": "user001", "name": "Alice", ... }                │
└──────────┬──────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────────────┐
│ User Service: Receives request                                  │
│ ✓ Creates user (in-memory)                                      │
│ → Returns 201 Created                                           │
│ → Publishes to user.events                                      │
└──────────┬──────────────────────────────────────────────────────┘
           │
           ▼ (milliseconds)
┌──────────────────────────────────────────────────────────────────┐
│ Kafka: user.events topic receives message                       │
│ Partitions: 1, Offset: 42                                       │
└──────────┬──────────────────────────────────────────────────────┘
           │
           ▼ (milliseconds)
┌──────────────────────────────────────────────────────────────────┐
│ Notification Service: UserEventConsumer triggers                │
│ Publishes to 3 topics:                                          │
│  ├─ email.events (Offset: 15)                                   │
│  ├─ sms.events (Offset: 10)                                     │
│  └─ push.events (Offset: 8)                                     │
└──────────┬────────────┬───────────────┬──────────────────────────┘
           │            │               │
    ┌──────▼──┐  ┌─────▼─┐  ┌────────┬─▼──┐
    │ Email   │  │ SMS   │  │ Push   │    │
    │ Consumer│  │ Consumer│ │Consumer│    │
    │Attempt 1│  │Attempt 1 │Attempt 1│    │
    └──┬──────┘  └────┬────┘ └───┬────┘    │
       │              │          │         │
       ▼              ▼          ▼         ▼
   ✓ SUCCESS    ✓ SUCCESS   ✓ SUCCESS
   (0ms delay) (0ms delay) (0ms delay)

   Message         Message        Message
   Acknowledged    Acknowledged   Acknowledged

Time < 100ms total: User has received all 3 notifications! 🎉
```

## Message Flow: Retry Path (Email Fails Once)

```
Time 0s: email.events arrives
         │
         ├─ EmailConsumer triggered
         │  └─ EmailService.sendEmail()
         │     └─ FAILS (random 10% failure)
         │
         └─ Exception caught
            └─ Framework re-queues to retry topic
               └─ Scheduled for 1 second later

Time 1s: email.events-retry arrives
         │
         ├─ EmailConsumer triggered again
         │  └─ EmailService.sendEmail()
         │     └─ SUCCESS ✓
         │
         └─ Message acknowledged

Total time: ~1-2 seconds ✓

User still gets the email, just with slight delay!
```

## Message Flow: DLQ Path (All Retries Fail)

```
Time 0s: email.events arrives
         │
         ├─ Attempt 1 @ 0s   → FAIL
         │  └─ Wait 1s
         │
         ├─ Attempt 2 @ 1s   → FAIL
         │  └─ Wait 2s
         │
         ├─ Attempt 3 @ 3s   → FAIL
         │  └─ All attempts exhausted
         │
         └─ Send to DLQ
            └─ email.events-dlt

DLQ Consumer:
├─ Receives message from email.events-dlt
├─ Logs: "[DLQ-EMAIL] Message failed after all retries! User: user001"
├─ Sends alert to Slack: "⚠️ Email delivery failed for user001"
├─ Creates PagerDuty incident: "Email service unavailable"
└─ Stores in database for manual review

Time 3-5s: On-call engineer notified 🚨
           Can manually review and retry later
```

## Independent Failure Domains

```
SCENARIO: SMS Service is temporarily down

Time 0s:
┌─────────────┐  ┌─────────────┐  ┌──────────────┐
│ Email       │  │ SMS         │  │ Push         │
│ Processing  │  │ Processing  │  │ Processing   │
│             │  │             │  │              │
│ Service UP  │  │ Service DOWN│  │ Service UP   │
└──────┬──────┘  └─────┬───────┘  └────────┬─────┘
       │                │                   │
       ▼                ▼                   ▼
   ✓ SUCCESS      ✗ FAIL (attempt 1)   ✓ SUCCESS
                  └─ Wait 1s
                  ✗ FAIL (attempt 2)
                  └─ Wait 2s
                  ✗ FAIL (attempt 3)
                  └─ Send to DLQ
                     (Still trying every 1-2s)

Time 1-2s:
┌──────────────────────────────────────────────────┐
│ User has EMAIL ✓ and PUSH ✓ notifications!       │
│ SMS will be retried up to 3 times                │
│ If SMS service comes back online, SMS succeeds!  │
│ If SMS service stays down, goes to DLQ           │
└──────────────────────────────────────────────────┘

Result: ✅ No cascading failure!
```

## Consumer Groups and Topics

```
┌──────────────────────────────────────────────────────┐
│ KAFKA CLUSTER                                        │
├──────────────────────────────────────────────────────┤
│                                                      │
│ Topic: user.events                                   │
│ ├─ Partition 0 ──────────┐                          │
│ ├─ Partition 1 ──────────┤ Offsets: 0-100           │
│ └─ Partition 2 ──────────┘                          │
│    Consumer Group: notification-service-group       │
│    └─ Status: All partitions assigned               │
│    └─ Lag: 0 (caught up)                            │
│                                                      │
├──────────────────────────────────────────────────────┤
│                                                      │
│ Topic: email.events                                  │
│ └─ Partition 0 ────────── Offsets: 0-47             │
│    Consumer Group: email-notification-consumer-grp  │
│    └─ Status: Assigned                              │
│    └─ Lag: 0 (caught up)                            │
│                                                      │
│ Topic: email.events-retry                           │
│ └─ Partition 0 ────────── Offsets: 0-3              │
│    Consumer Group: email-notification-consumer-grp  │
│    └─ Status: Assigned                              │
│    └─ Lag: 0 (caught up)                            │
│                                                      │
│ Topic: email.events-dlt (Dead Letter)               │
│ └─ Partition 0 ────────── Offsets: 0-0 (empty)      │
│    Consumer Group: dlq-email-consumer-group         │
│    └─ Status: Assigned                              │
│    └─ Lag: 0                                        │
│                                                      │
├──────────────────────────────────────────────────────┤
│                                                      │
│ Topic: sms.events                                    │
│ └─ Partition 0 ────────── Offsets: 0-47             │
│    Consumer Group: sms-notification-consumer-grp    │
│    └─ Lag: 0                                        │
│                                                      │
│ Topic: sms.events-retry                             │
│ └─ Partition 0 ────────── Offsets: 0-2              │
│                                                      │
│ Topic: sms.events-dlt (Dead Letter)                 │
│ └─ Partition 0 ────────── Offsets: 0-0 (empty)      │
│                                                      │
├──────────────────────────────────────────────────────┤
│                                                      │
│ Topic: push.events                                   │
│ └─ Partition 0 ────────── Offsets: 0-47             │
│    Consumer Group: push-notification-consumer-grp   │
│    └─ Lag: 0                                        │
│                                                      │
│ Topic: push.events-retry                            │
│ └─ Partition 0 ────────── Offsets: 0-1              │
│                                                      │
│ Topic: push.events-dlt (Dead Letter)                │
│ └─ Partition 0 ────────── Offsets: 0-0 (empty)      │
│                                                      │
└──────────────────────────────────────────────────────┘
```

## Scaling Architecture

```
BEFORE: Single Consumer (Bottleneck)
┌────────────────────┐
│ UserEventConsumer  │  ← Single thread, cannot scale
│ (Serializes all)   │  ← If processing email takes long,
│                    │    SMS and Push wait!
└────────────────────┘

AFTER: Independent Consumers (Scalable)
┌──────────────────┐
│ EmailConsumer    │  Replicas: 1  (Can scale to 2, 3, 4...)
│ (1 thread/rep)   │
└──────────────────┘

┌──────────────────┐
│ SmsConsumer      │  Replicas: 1  (Can scale to 2, 3, 4...)
│ (1 thread/rep)   │
└──────────────────┘

┌──────────────────┐
│ PushConsumer     │  Replicas: 1  (Can scale to 2, 3, 4...)
│ (1 thread/rep)   │
└──────────────────┘

Each consumer processes independently!
If email is slow, just add more email consumer replicas.
SMS and push are unaffected.
```

## Error Tracking Flow

```
     Success ✓
        │
        ▼
   Message Acked
        │
        ▼
   Notification Sent!


     Failure ✗
        │
        ├─ Log: "Attempt 1 failed"
        │
        ├─ Schedule Retry @ +1s
        │
        ▼
    Retry Failed ✗
        │
        ├─ Log: "Attempt 2 failed"
        │
        ├─ Schedule Retry @ +2s
        │
        ▼
    Final Retry Failed ✗
        │
        ├─ Log: "Attempt 3 failed, sending to DLQ"
        │
        ▼
   Message → DLQ
        │
        ▼
   DLQ Consumer
        │
        ├─ Alert Ops 🚨
        ├─ Log Details 📝
        ├─ Store in DB 💾
        │
        ▼
   Manual Intervention
        │
        ├─ Investigate root cause
        ├─ Fix underlying issue
        ├─ Manual retry from DB
        │
        ▼
   Notification Sent (Eventually) ✓
```

---

## Key Takeaways (Visual Summary)

```
PROBLEM SOLVED: Cascading Failures
══════════════════════════════════════════════════════════════

BEFORE (❌ Cascading):
   Email fails → SMS blocked → Push blocked → User angry 😞
   
AFTER (✅ Independent):
   Email fails → ✓ SMS succeeds → ✓ Push succeeds → User OK 😊
                  (Retry in background)


PROBLEM SOLVED: Lost Messages
══════════════════════════════════════════════════════════════

BEFORE (❌ Lost):
   Message fails → No record → No way to recover 🚫
   
AFTER (✅ Saved):
   Message fails 3x → Saved to DLQ → Can be retried later ✓


PROBLEM SOLVED: Visibility
══════════════════════════════════════════════════════════════

BEFORE (❌ Blind):
   "Why didn't user get email?" → Can't tell 🤷
   
AFTER (✅ Visible):
   logs →
   [EMAIL] Attempt 1: FAIL
   [EMAIL] Attempt 2: SUCCESS ✓
   
   Or:
   
   logs →
   [EMAIL] Attempt 1: FAIL
   [EMAIL] Attempt 2: FAIL
   [EMAIL] Attempt 3: FAIL
   [DLQ] Sent to dead-letter queue
   
   Now you know exactly what happened! 🎯
```
