package com.example.notificationservice.service;

import com.example.notificationservice.model.NotificationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Email Notification Service - Sends email notifications
 * In production, this would integrate with SendGrid, AWS SES, etc.
 */
@Service
public class EmailNotificationService {
    
    private static final Logger logger = LoggerFactory.getLogger(EmailNotificationService.class);
    
    /**
     * Send email notification
     * @param event Notification event containing user details
     * @throws EmailNotificationException if email sending fails
     */
    public void sendEmail(NotificationEvent event) throws EmailNotificationException {
        
        if (event == null || event.getEmail() == null) {
            throw new EmailNotificationException("Email address is required");
        }
        
        logger.info("[EMAIL SERVICE] Processing email for user: {} ({})", 
            event.getUserId(), event.getEmail());
        
        try {
            // Simulate email sending with 10% failure rate (for demo)
            if (Math.random() < 0.1) {
                throw new EmailNotificationException(
                    "Email server temporarily unavailable for: " + event.getEmail());
            }
            
            // In production, you would:
            // emailClient.send(new Email()
            //     .setTo(event.getEmail())
            //     .setSubject("Welcome " + event.getName())
            //     .setBody("Thank you for registering..."));
            
            logger.info("[EMAIL SERVICE] ✓ Email sent successfully to: {}", event.getEmail());
            
        } catch (Exception e) {
            logger.error("[EMAIL SERVICE] ✗ Failed to send email to: {}", event.getEmail(), e);
            throw new EmailNotificationException("Failed to send email: " + e.getMessage(), e);
        }
    }
    
    /**
     * Custom exception for email notification failures
     */
    public static class EmailNotificationException extends Exception {
        public EmailNotificationException(String message) {
            super(message);
        }
        
        public EmailNotificationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
