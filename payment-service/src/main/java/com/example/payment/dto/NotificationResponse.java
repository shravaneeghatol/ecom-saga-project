package com.example.payment.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class NotificationResponse {
    private boolean success;
    private String notificationId;
    private String message;
}
