package com.example.order.dto;

import lombok.Data;

@Data
public class CreateOrderRequest {
    private String customerId;
    private String productId;
    private Integer quantity;
    private Double amount;

    // --- test/demo controls to deliberately trigger saga failure paths ---

    /** Forces inventory-service to report "insufficient stock" (immediate business failure, no retries). */
    private boolean simulateInventoryFailure = false;

    /** Forces payment-service to decline the payment (immediate business failure, no retries). */
    private boolean simulatePaymentFailure = false;

    /**
     * Forces the named service to throw a transient exception on every attempt,
     * exercising the retry topics -> DLT path. One of: "inventory", "payment", "notification", or null.
     */
    private String simulateTransientErrorAt;
}
