package com.example.notification.service.senderclients;

import org.springframework.stereotype.Component;

@Component
public class EmailSender implements SenderClient {
    @Override
    public void send(String recipient, String subject, String message) {
        // TODO: Implement email sending
        System.out.println("Sending email to " + recipient + " with subject " + subject + " and message " + message);
    }
    @Override
    public boolean supportsChannel(String channelName) {
        return "EMAIL".equalsIgnoreCase(channelName);
    }
}
