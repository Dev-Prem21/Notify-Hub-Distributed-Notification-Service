package com.example.userservice.kafka;

import com.example.userservice.model.User;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class UserEventProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String TOPIC = "user.events";

    public UserEventProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendUserRegistered(User user) {
        var event = new UserRegisteredEvent(user.getId(), user.getName(), user.getEmail());
        kafkaTemplate.send(TOPIC, event);
    }

    static class UserRegisteredEvent {
        private String id;
        private String name;
        private String email;

        public UserRegisteredEvent() {
        }

        public UserRegisteredEvent(String id, String name, String email) {
            this.id = id;
            this.name = name;
            this.email = email;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }
    }
}
