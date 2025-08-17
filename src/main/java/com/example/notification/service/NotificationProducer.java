package com.example.notification.service;

import java.time.Instant;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.example.notification.model.NotificationEvent;
import com.example.notification.model.NotificationRequest;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class NotificationProducer {

    private final KafkaTemplate<String, NotificationEvent> kafkaTemplate;

    @Value("${app.kafka.topic.notifications}")
    private String topic;

    public String publish(NotificationRequest request) {
        NotificationEvent event = NotificationEvent.builder()
                .id(UUID.randomUUID().toString())
                .recipient(request.getRecipient())
                .channel(request.getChannel())
                .subject(request.getSubject())
                .message(request.getMessage())
                .metadata(request.getMetadata())
                .createdAt(Instant.now())
                .attempt(0)
                .build();
        // This is the kafka template that sends the event to the kafka topic
        // function definition: send(String topic, String key, T data)
        kafkaTemplate.send(topic, event.getId(), event);
        // This is the id of the event
        return event.getId();
    }
}


