package com.example.notification.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.example.notification.model.NotificationEvent;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);
    private final NotificationRouter router;

    @KafkaListener(topics = "${app.kafka.topic.notifications}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void onMessage(@Payload NotificationEvent event) throws Exception {
        log.info("Processing notification id={}, channel={}, recipient={}", event.getId(), event.getChannel(), event.getRecipient());
        // Demo failure toggle
        if (event.getSubject() != null && event.getSubject().toLowerCase().contains("fail")) {
            throw new IllegalStateException("Simulated processing failure for subject='" + event.getSubject() + "'");
        }
        router.route(event);
        log.info("Notification processed id={}", event.getId());
    }
}


