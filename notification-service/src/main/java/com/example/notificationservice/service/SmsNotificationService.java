package com.example.notificationservice.service;

import com.example.notificationservice.model.NotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * SMS Notification Service - Sends SMS notifications
 * In production, this would integrate with Twilio, AWS SNS, etc.
 */
@Service
public class SmsNotificationService {
    
    private static final Logger logger = LoggerFactory.getLogger(SmsNotificationService.class);
    
    /**
     * Send SMS notification
     * @param event Notification event containing user details
     * @throws SmsNotificationException if SMS sending fails
     */
    public void sendSms(NotificationEvent event) throws SmsNotificationException {
        
        if (event == null || event.getPhoneNumber() == null) {
            throw new SmsNotificationException("Phone number is required");
        }
        
        logger.info("[SMS SERVICE] Processing SMS for user: {} ({})", 
            event.getUserId(), event.getPhoneNumber());
        
        try {
            // Simulate SMS sending with 15% failure rate (for demo)
            if (Math.random() < 0.15) {
                throw new SmsNotificationException(
                    "SMS gateway error for: " + event.getPhoneNumber());
            }
            
            // In production, you would:
            // twilioClient.messages()
            //     .create(new CreateMessageRequest()
            //         .setTo(event.getPhoneNumber())
            //         .setFrom("+1234567890")
            //         .setBody("Welcome " + event.getName() + "! Thanks for registering."));
            
            logger.info("[SMS SERVICE] ✓ SMS sent successfully to: {}", event.getPhoneNumber());
            
        } catch (Exception e) {
            logger.error("[SMS SERVICE] ✗ Failed to send SMS to: {}", event.getPhoneNumber(), e);
            throw new SmsNotificationException("Failed to send SMS: " + e.getMessage(), e);
        }
    }
    
    /**
     * Custom exception for SMS notification failures
     */
    public static class SmsNotificationException extends Exception {
        public SmsNotificationException(String message) {
            super(message);
        }
        
        public SmsNotificationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
