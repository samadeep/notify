package com.example.notification.controller;

import java.util.Map;

import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.notification.model.NotificationRequest;
import com.example.notification.service.NotificationProducer;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/notifications")
@Validated
public class NotificationController {

    private final NotificationProducer notificationProducer;

    public NotificationController(NotificationProducer notificationProducer) {
        this.notificationProducer = notificationProducer;
    }

    @PostMapping
    public Map<String, String> sendNotification(@Valid @RequestBody NotificationRequest request) {
        String id = notificationProducer.publish(request);
        return Map.of("id", id);
    }
}