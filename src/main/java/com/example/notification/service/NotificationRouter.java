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
        // This is the list of senders
        for (NotificationSender sender : senders) {
            // This is the sender that supports the channel
            if (sender.supportsChannel(event.getChannel().name())) {
                sender.send(event);
                return;
            }
        }
        // This is the exception that is thrown if no sender is found for the channel
        throw new IllegalArgumentException("No sender found for channel " + event.getChannel());
    }
}


