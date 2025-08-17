package com.example.notification.model;

import java.time.Instant;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationEvent {
    private String id;
    private String recipient;
    private Channel channel;
    private String subject;
    private String message;
    private Map<String, Object> metadata;
    private Instant createdAt;
    private int attempt;
}


