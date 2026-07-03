package com.example.notification.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class EmailRequest {
    private String to;
    private String subject;
    private String body;
    private String from;
}
