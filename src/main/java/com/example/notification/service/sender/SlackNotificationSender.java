package com.example.notification.service.sender;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.example.notification.model.NotificationEvent;    

@Component
public class SlackNotificationSender implements NotificationSender {

    private static final Logger log = LoggerFactory.getLogger(SlackNotificationSender.class);

    @Override
    public boolean supportsChannel(String channelName) {
        return "SLACK".equalsIgnoreCase(channelName);
    }

    @Override
    public void send(NotificationEvent event) {
        String slackChannel = event.getRecipient();
        log.info("[SLACK] Channel={}, Subject={}, Message={}", slackChannel, event.getSubject(), event.getMessage());
        // In a real implementation, post to Slack via webhook or Bot token.
        // Optionally use event.getMetadata() to pass webhookUrl, threadTs, etc.
    }
}
