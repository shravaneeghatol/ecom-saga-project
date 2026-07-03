package com.example.payment.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class OrderCompensationRequest {
    private String orderId;
    private String reason;
}