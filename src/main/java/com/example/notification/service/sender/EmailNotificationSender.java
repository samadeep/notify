package com.example.notification.service.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.example.notification.model.NotificationEvent;

@Component
public class EmailNotificationSender implements NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(EmailNotificationSender.class);

    @Override
    public boolean supportsChannel(String channelName) {
        return "EMAIL".equalsIgnoreCase(channelName);
    }

    @Override
    public void send(NotificationEvent event) {
        log.info("[EMAIL] To={}, Subject={}", event.getRecipient(), event.getSubject());
    }
}


