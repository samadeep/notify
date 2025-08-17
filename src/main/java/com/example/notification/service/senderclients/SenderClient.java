package com.example.notification.service.senderclients;

public interface SenderClient {
    void send(String recipient, String subject, String message);
    boolean supportsChannel(String channelName);
}
