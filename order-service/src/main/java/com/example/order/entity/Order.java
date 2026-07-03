package com.example.order.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Order {

    @Id
    @GeneratedValue
    private UUID id;

    private String customerId;
    private String productId;
    private Integer quantity;
    private Double amount;

    @Enumerated(EnumType.STRING)
    private OrderStatus status;

    private String failureReason;

    private Instant createdAt;
    private Instant updatedAt;

    @Column(name = "total_amount")
    private Double totalAmount;

    @PrePersist
    public void prePersist() {
        createdAt = Instant.now();
        updatedAt = Instant.now();
        if (status == null) status = OrderStatus.CREATED;
        if (totalAmount == null && amount != null) {
            totalAmount = amount;
        }
    }

    @PreUpdate
    public void preUpdate() {
        updatedAt = Instant.now();
    }
}