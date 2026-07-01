package com.example.notificationservice.kafka;

import com.example.notificationservice.model.NotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Dead Letter Queue (DLQ) Consumer
 * Handles messages that failed even after retries
 * 
 * DLQ Topics:
 * - email.events-dlt (for email failures)
 * - sms.events-dlt (for SMS failures)
 * - push.events-dlt (for push failures)
 * 
 * These messages are here because:
 * 1. The service was down (all retries exhausted)
 * 2. There's a permanent error (invalid data, etc.)
 * 3. The service rejected the message
 * 
 * In production, you would:
 * - Alert monitoring/on-call team
 * - Store in database for manual review
 * - Send to Slack/PagerDuty
 */
@Component
public class DeadLetterQueueConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(DeadLetterQueueConsumer.class);
    
    /**
     * Handle failed email notifications
     */
    @KafkaListener(topics = "email.events-dlt", groupId = "dlq-email-consumer-group")
    public void handleEmailDlq(
            @Payload NotificationEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = "kafka_exception_message", required = false) String exceptionMessage) {
        
        logger.error("[DLQ-EMAIL] Message failed after all retries! Topic: {}, User: {}, Email: {}, Exception: {}", 
            topic, event.getUserId(), event.getEmail(), exceptionMessage);
        
        // TODO: In production, implement:
        // 1. Store in database for manual review
        // 2. Send alert to Slack channel
        // 3. Create ticket in PagerDuty
        // 4. Send email to ops team
        
        handleDlqMessage(event, "EMAIL", topic);
    }
    
    /**
     * Handle failed SMS notifications
     */
    @KafkaListener(topics = "sms.events-dlt", groupId = "dlq-sms-consumer-group")
    public void handleSmsDlq(
            @Payload NotificationEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = "kafka_exception_message", required = false) String exceptionMessage) {
        
        logger.error("[DLQ-SMS] Message failed after all retries! Topic: {}, User: {}, Phone: {}, Exception: {}", 
            topic, event.getUserId(), event.getPhoneNumber(), exceptionMessage);
        
        handleDlqMessage(event, "SMS", topic);
    }
    
    /**
     * Handle failed push notifications
     */
    @KafkaListener(topics = "push.events-dlt", groupId = "dlq-push-consumer-group")
    public void handlePushDlq(
            @Payload NotificationEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(name = "kafka_exception_message", required = false) String exceptionMessage) {
        
        logger.error("[DLQ-PUSH] Message failed after all retries! Topic: {}, User: {}, Exception: {}", 
            topic, event.getUserId(), exceptionMessage);
        
        handleDlqMessage(event, "PUSH", topic);
    }
    
    /**
     * Common DLQ handling logic
     */
    private void handleDlqMessage(NotificationEvent event, String notificationType, String topic) {
        logger.warn("[DLQ] Processing {} notification failure for user: {}", notificationType, event.getUserId());
        
        // Alert Operations Team
        alertOpsTeam(notificationType, event);
        
        // Log for audit trail
        logFailureDetails(notificationType, event);
        
        // Could also:
        // - Store in database for manual retry later
        // - Send notification via alternative channel
        // - Trigger escalation workflow
    }
    
    private void alertOpsTeam(String notificationType, NotificationEvent event) {
        String alertMessage = String.format(
            "🚨 ALERT: %s notification delivery failed for user %s after 3 retries. " +
            "Manual intervention may be required. User: %s, Email: %s, Phone: %s",
            notificationType, event.getUserId(), event.getName(), event.getEmail(), event.getPhoneNumber()
        );
        
        logger.error("[ALERT] {}", alertMessage);
        
        // TODO: Send to monitoring system
        // alertingService.sendAlert(alertMessage);
        // slackClient.sendMessage(alertMessage);
        // pagerDutyClient.triggerIncident(alertMessage);
    }
    
    private void logFailureDetails(String notificationType, NotificationEvent event) {
        logger.info("[DLQ-LOG] Failure Details - Type: {}, UserId: {}, Name: {}, Email: {}, Phone: {}, EventType: {}",
            notificationType,
            event.getUserId(),
            event.getName(),
            event.getEmail(),
            event.getPhoneNumber(),
            event.getEventType()
        );
    }
}
