package com.example.inventory.dto;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class PaymentRequest {
    private String orderId;
    private Double amount;
    private String customerId;
}
