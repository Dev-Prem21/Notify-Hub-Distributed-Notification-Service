package com.example.notificationservice.kafka;

import com.example.notificationservice.model.NotificationEvent;
import com.example.notificationservice.service.PushNotificationService;
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
 * Push Notification Consumer
 * Consumes push notification events and sends push notifications
 * 
 * Topic: push.events
 * Consumer Group: push-notification-consumer-group
 * Retry Topic: push.events-retry
 * DLQ Topic: push.events-dlt
 */
@Component
public class PushNotificationConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(PushNotificationConsumer.class);
    
    private final PushNotificationService pushService;
    
    public PushNotificationConsumer(PushNotificationService pushService) {
        this.pushService = pushService;
    }
    
    @RetryableTopic(
        attempts = "3",
        delay = "1000",
        multiplier = "2.0",
        autoCreateTopic = "true",
        include = {PushNotificationService.PushNotificationException.class},
        dltStrategy = org.springframework.kafka.retrytopic.DltStrategy.FAIL_ON_ERROR
    )
    @KafkaListener(topics = "push.events", groupId = "push-notification-consumer-group")
    public void handlePushNotification(
            @Payload NotificationEvent event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        
        logger.info("[PUSH CONSUMER] Received message from topic: {}, partition: {}, offset: {} | Event: {}", 
            topic, partition, offset, event);
        
        try {
            pushService.sendPush(event);
            logger.info("[PUSH CONSUMER] ✓ Successfully processed push notification for user: {}", event.getUserId());
            
        } catch (PushNotificationService.PushNotificationException e) {
            logger.error("[PUSH CONSUMER] ✗ Failed to send push notification for user: {}. Will retry.", event.getUserId(), e);
            throw new RuntimeException("Push notification failed, will retry", e);
        }
    }
}
