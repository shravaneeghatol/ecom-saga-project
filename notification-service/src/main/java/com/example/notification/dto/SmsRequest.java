package com.example.notification.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class SmsRequest {
    private String phoneNumber;
    private String message;
    private String sender;
}