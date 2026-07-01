package com.example.notificationservice.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/events")
public class EventController {

    @PostMapping
    public ResponseEntity<Map<String, Object>> publish(@RequestBody Map<String, Object> payload) {
        // For demo: we accept event publish but do not send to Kafka here
        return ResponseEntity.accepted().body(payload);
    }
}
