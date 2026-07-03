package com.example.order.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CreateOrderRequest {
    private String productId;
    private Integer quantity;
    private String customerId;
    private Double totalAmount;

    // Getter for amount (used by OrderService)
    public Double getAmount() {
        return this.totalAmount;
    }

    // Setter for amount (maps to totalAmount)
    public void setAmount(Double amount) {
        this.totalAmount = amount;
    }
}