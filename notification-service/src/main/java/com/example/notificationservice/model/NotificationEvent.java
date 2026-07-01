package com.example.notificationservice.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Base notification event that gets routed to different queues
 */
public class NotificationEvent {
    
    @JsonProperty("userId")
    private String userId;
    
    @JsonProperty("email")
    private String email;
    
    @JsonProperty("phoneNumber")
    private String phoneNumber;
    
    @JsonProperty("name")
    private String name;
    
    @JsonProperty("eventType")
    private String eventType;

    public NotificationEvent() {
    }

    public NotificationEvent(String userId, String email, String phoneNumber, String name, String eventType) {
        this.userId = userId;
        this.email = email;
        this.phoneNumber = phoneNumber;
        this.name = name;
        this.eventType = eventType;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @Override
    public String toString() {
        return "NotificationEvent{" +
                "userId='" + userId + '\'' +
                ", email='" + email + '\'' +
                ", phoneNumber='" + phoneNumber + '\'' +
                ", name='" + name + '\'' +
                ", eventType='" + eventType + '\'' +
                '}';
    }
}
