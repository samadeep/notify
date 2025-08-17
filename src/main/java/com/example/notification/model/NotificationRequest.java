package com.example.notification.model;

import java.util.Map;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class NotificationRequest {

    @NotBlank
    private String recipient;

    @NotNull
    private Channel channel;

    @NotBlank
    private String subject;

    @NotBlank
    private String message;

    private Map<String, Object> metadata;
}


