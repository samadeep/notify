package com.example.notification.service.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.example.notification.model.NotificationEvent;

@Component
public class SmsNotificationSender implements NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(SmsNotificationSender.class);

    @Override
    public boolean supportsChannel(String channelName) {
        return "SMS".equalsIgnoreCase(channelName);
    }

    @Override
    public void send(NotificationEvent event) {
        log.info("[SMS] To={}, Message={}", event.getRecipient(), event.getMessage());
    }
}


