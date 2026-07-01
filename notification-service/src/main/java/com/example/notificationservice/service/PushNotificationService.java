package com.example.notificationservice.service;

import com.example.notificationservice.model.NotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Push Notification Service - Sends push notifications
 * In production, this would integrate with Firebase Cloud Messaging, Apple Push, etc.
 */
@Service
public class PushNotificationService {
    
    private static final Logger logger = LoggerFactory.getLogger(PushNotificationService.class);
    
    /**
     * Send push notification
     * @param event Notification event containing user details
     * @throws PushNotificationException if push sending fails
     */
    public void sendPush(NotificationEvent event) throws PushNotificationException {
        
        if (event == null || event.getUserId() == null) {
            throw new PushNotificationException("User ID is required");
        }
        
        logger.info("[PUSH SERVICE] Processing push notification for user: {}", event.getUserId());
        
        try {
            // Simulate push notification with 5% failure rate (most reliable channel)
            if (Math.random() < 0.05) {
                throw new PushNotificationException(
                    "Push service error for user: " + event.getUserId());
            }
            
            // In production, you would:
            // firebaseMessaging.send(new Message()
            //     .setToken(userDevice.getToken())
            //     .setNotification(new Notification()
            //         .setTitle("Welcome " + event.getName())
            //         .setBody("Thanks for registering!")));
            
            logger.info("[PUSH SERVICE] ✓ Push notification sent successfully to user: {}", event.getUserId());
            
        } catch (Exception e) {
            logger.error("[PUSH SERVICE] ✗ Failed to send push notification to user: {}", event.getUserId(), e);
            throw new PushNotificationException("Failed to send push notification: " + e.getMessage(), e);
        }
    }
    
    /**
     * Custom exception for push notification failures
     */
    public static class PushNotificationException extends Exception {
        public PushNotificationException(String message) {
            super(message);
        }
        
        public PushNotificationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
