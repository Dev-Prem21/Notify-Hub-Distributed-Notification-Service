package com.example.notificationservice.kafka;

import com.example.notificationservice.model.NotificationEvent;
import com.example.notificationservice.service.SmsNotificationService;
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
 * SMS Notification Consumer
 * Consumes SMS notification events and sends SMS messages
 * 
 * Topic: sms.events
 * Consumer Group: sms-notification-consumer-group
 * Retry Topic: sms.events-retry
 * DLQ Topic: sms.events-dlt
 */
@Component
public class SmsNotificationConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(SmsNotificationConsumer.class);
    
    private final SmsNotificationService smsService;
    
    public SmsNotificationConsumer(SmsNotificationService smsService) {
        this.smsService = smsService;
    }
    
    @RetryableTopic(
        attempts = "3",
        delay = "1000",
        multiplier = "2.0",
        autoCreateTopic = "true",
        include = {SmsNotificationService.SmsNotificationException.class},
        dltStrategy = org.springframework.kafka.retrytopic.DltStrategy.FAIL_ON_ERROR
    )
    @KafkaListener(topics = "sms.events", groupId = "sms-notification-consumer-group")
    public void handleSmsNotification(
            @Payload NotificationEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        
        logger.info("[SMS CONSUMER] Received message from topic: {}, partition: {}, offset: {} | Event: {}", 
            topic, partition, offset, event);
        
        try {
            smsService.sendSms(event);
            logger.info("[SMS CONSUMER] ✓ Successfully processed SMS notification for user: {}", event.getUserId());
            
        } catch (SmsNotificationService.SmsNotificationException e) {
            logger.error("[SMS CONSUMER] ✗ Failed to send SMS for user: {}. Will retry.", event.getUserId(), e);
            throw new RuntimeException("SMS notification failed, will retry", e);
        }
    }
}
