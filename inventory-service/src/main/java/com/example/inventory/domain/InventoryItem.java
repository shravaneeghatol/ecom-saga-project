package com.example.inventory.domain;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

@Entity
@Table(name = "inventory_item")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class InventoryItem {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(unique = true)
    private String productId;

    private Integer availableQty;
    private Integer reservedQty;
}
