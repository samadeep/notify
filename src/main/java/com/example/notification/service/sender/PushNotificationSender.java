package com.example.notification.service.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.example.notification.model.NotificationEvent;

@Component
public class PushNotificationSender implements NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(PushNotificationSender.class);

    @Override
    public boolean supportsChannel(String channelName) {
        return "PUSH".equalsIgnoreCase(channelName);
    }

    @Override
    public void send(NotificationEvent event) throws Exception {
        log.info("[PUSH] To={}, Message={}", event.getRecipient(), event.getMessage());
    }
}


