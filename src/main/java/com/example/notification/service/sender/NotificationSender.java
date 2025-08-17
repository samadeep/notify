package com.example.notification.service.sender;

import com.example.notification.model.NotificationEvent;

public interface NotificationSender {
    boolean supportsChannel(String channelName);
    void send(NotificationEvent event) throws Exception;
}


