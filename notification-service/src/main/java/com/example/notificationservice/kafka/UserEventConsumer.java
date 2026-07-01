package com.example.notificationservice.kafka;

import com.example.notificationservice.model.NotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * User Event Consumer - Routes user registration events to specific notification topics
 * 
 * This is the EVENT ROUTER/DISPATCHER:
 * - Listens to user.events topic (events from User Service)
 * - Routes each event to THREE separate topics:
 *   1. email.events (for Email Notification Consumer)
 *   2. sms.events (for SMS Notification Consumer)
 *   3. push.events (for Push Notification Consumer)
 * 
 * Architecture:
 * 
 *     User Service
 *            │
 *            ├─── user.events (Kafka topic)
 *                      │
 *                      ▼
 *            UserEventConsumer (Router)
 *                      │
 *         ┌────────────┼────────────┐
 *         │            │            │
 *         ▼            ▼            ▼
 *    email.events  sms.events  push.events
 *         │            │            │
 *         ▼            ▼            ▼
 *  EmailConsumer  SmsConsumer  PushConsumer
 * 
 * Benefits of this approach:
 * - Each notification type has its own consumer group
 * - Each consumer can scale independently
 * - If one fails, others are not affected
 * - Each has its own retry logic and DLQ
 * - Decouples notification types from each other
 */
@Component
public class UserEventConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(UserEventConsumer.class);
    
    private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;
    
    // Topics for routing notifications
    private static final String EMAIL_TOPIC = "email.events";
    private static final String SMS_TOPIC = "sms.events";
    private static final String PUSH_TOPIC = "push.events";
    
    public UserEventConsumer(KafkaTemplate<String, NotificationEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Handles user registration events and routes them to all notification channels
     * 
     * @param userEvent The user registration event from user.events topic
     */
    @KafkaListener(topics = "user.events", groupId = "notification-service-group")
    public void handleUserRegistered(
            @Payload Object userEvent,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        
        logger.info("[USER-EVENT-ROUTER] Received user event from topic: {}, partition: {}, offset: {} | Event: {}", 
            topic, partition, offset, userEvent);
        
        try {
            // Convert the raw event to NotificationEvent
            NotificationEvent notificationEvent = convertToNotificationEvent(userEvent);
            
            // Route to all notification channels independently
            routeEmailNotification(notificationEvent);
            routeSmsNotification(notificationEvent);
            routePushNotification(notificationEvent);
            
            logger.info("[USER-EVENT-ROUTER] ✓ Successfully routed event for user: {} to all notification channels", 
                notificationEvent.getUserId());
            
        } catch (Exception e) {
            logger.error("[USER-EVENT-ROUTER] ✗ Failed to route user event: {}", userEvent, e);
            // Don't rethrow - we don't want to retry this (routing layer shouldn't fail)
            // The consumer topics will handle retries
        }
    }

    /**
     * Routes the notification event to the email notification topic
     */
    private void routeEmailNotification(NotificationEvent event) {
        try {
            kafkaTemplate.send(EMAIL_TOPIC, event.getUserId(), event);
            logger.debug("[ROUTER] ✓ Routed event to email.events for user: {}", event.getUserId());
        } catch (Exception e) {
            logger.error("[ROUTER] ✗ Failed to route to email.events for user: {}", event.getUserId(), e);
        }
    }

    /**
     * Routes the notification event to the SMS notification topic
     */
    private void routeSmsNotification(NotificationEvent event) {
        try {
            kafkaTemplate.send(SMS_TOPIC, event.getUserId(), event);
            logger.debug("[ROUTER] ✓ Routed event to sms.events for user: {}", event.getUserId());
        } catch (Exception e) {
            logger.error("[ROUTER] ✗ Failed to route to sms.events for user: {}", event.getUserId(), e);
        }
    }

    /**
     * Routes the notification event to the push notification topic
     */
    private void routePushNotification(NotificationEvent event) {
        try {
            kafkaTemplate.send(PUSH_TOPIC, event.getUserId(), event);
            logger.debug("[ROUTER] ✓ Routed event to push.events for user: {}", event.getUserId());
        } catch (Exception e) {
            logger.error("[ROUTER] ✗ Failed to route to push.events for user: {}", event.getUserId(), e);
        }
    }

    /**
     * Converts a raw user event to a NotificationEvent
     * Handles both direct NotificationEvent objects and Map objects
     */
    private NotificationEvent convertToNotificationEvent(Object userEvent) {
        if (userEvent instanceof NotificationEvent) {
            return (NotificationEvent) userEvent;
        }
        
        if (userEvent instanceof java.util.Map) {
            java.util.Map<String, Object> map = (java.util.Map<String, Object>) userEvent;
            return new NotificationEvent(
                (String) map.getOrDefault("id", "unknown"),
                (String) map.getOrDefault("email", "unknown@example.com"),
                (String) map.getOrDefault("phoneNumber", "+1-000-0000"),
                (String) map.getOrDefault("name", "User"),
                "USER_REGISTERED"
            );
        }
        
        // Fallback: create a default notification event
        logger.warn("[CONVERTER] Unknown event type: {}, creating default event", userEvent.getClass());
        return new NotificationEvent(
            "unknown",
            "unknown@example.com",
            "+1-000-0000",
            "Unknown User",
            "USER_REGISTERED"
        );
    }
}
