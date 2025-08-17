package com.example.notification.service;

import java.util.List;

import org.springframework.stereotype.Service;

import com.example.notification.model.NotificationEvent;
import com.example.notification.service.sender.NotificationSender;

import lombok.RequiredArgsConstructor;

@Service
@RequiredArgsConstructor
public class NotificationRouter {

    private final List<NotificationSender> senders;

    public void route(NotificationEvent event) throws Exception {
        for (NotificationSender sender : senders) {
            if (sender.supportsChannel(event.getChannel().name())) {
                sender.send(event);
                return;
            }
        }
        throw new IllegalArgumentException("No sender found for channel " + event.getChannel());
    }
}


