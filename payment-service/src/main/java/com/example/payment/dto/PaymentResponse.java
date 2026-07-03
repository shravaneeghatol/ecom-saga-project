package com.example.payment.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PaymentResponse {
    private boolean success;
    private String transactionId;
    private String message;
}

