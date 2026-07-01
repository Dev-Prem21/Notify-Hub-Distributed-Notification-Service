package com.example.notificationservice.kafka;

import com.example.notificationservice.model.NotificationEvent;
import com.example.notificationservice.service.EmailNotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.retrytopic.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * Email Notification Consumer
 * Consumes email notification events and sends emails
 * 
 * Topic: email.events
 * Consumer Group: email-notification-consumer-group
 * Retry Topic: email.events-retry
 * DLQ Topic: email.events-dlt
 */
@Component
public class EmailNotificationConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(EmailNotificationConsumer.class);
    
    private final EmailNotificationService emailService;
    
    public EmailNotificationConsumer(EmailNotificationService emailService) {
        this.emailService = emailService;
    }
    
    @RetryableTopic(
        attempts = "3",
        delay = "1000",
        multiplier = "2.0",
        autoCreateTopic = "true",
        include = {EmailNotificationService.EmailNotificationException.class},
        dltStrategy = org.springframework.kafka.retrytopic.DltStrategy.FAIL_ON_ERROR
    )
    @KafkaListener(topics = "email.events", groupId = "email-notification-consumer-group")
    public void handleEmailNotification(
            @Payload NotificationEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        
        logger.info("[EMAIL CONSUMER] Received message from topic: {}, partition: {}, offset: {} | Event: {}", 
            topic, partition, offset, event);
        
        try {
            emailService.sendEmail(event);
            logger.info("[EMAIL CONSUMER] ✓ Successfully processed email notification for user: {}", event.getUserId());
            
        } catch (EmailNotificationService.EmailNotificationException e) {
            logger.error("[EMAIL CONSUMER] ✗ Failed to send email for user: {}. Will retry.", event.getUserId(), e);
            throw new RuntimeException("Email notification failed, will retry", e);
        }
    }
}
