package com.example.notification.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SmsResponse {
    private boolean success;
    private String messageId;
    private String message;
}