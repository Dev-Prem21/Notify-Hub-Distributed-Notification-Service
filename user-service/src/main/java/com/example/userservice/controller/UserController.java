package com.example.userservice.controller;

import com.example.userservice.model.User;
import com.example.userservice.kafka.UserEventProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/users")
public class UserController {

    private final UserEventProducer producer;

    public UserController(UserEventProducer producer) {
        this.producer = producer;
    }

    @PostMapping
    public ResponseEntity<User> register(@RequestBody User user) {
        // In-memory stub: normally you'd persist the user
        producer.sendUserRegistered(user);
        return ResponseEntity.status(201).body(user);
    }
}
