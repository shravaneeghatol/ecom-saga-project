package com.example.payment.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class OrderCompensationResponse {
    private boolean success;
    private String message;
}
