package com.example.order.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * The Outbox table. Business state changes and the outbox row are written
 * in the SAME local transaction, so we never lose an event even if Kafka
 * is temporarily unreachable (solves the "dual write" problem).
 * A separate poller (OutboxPublisher) asynchronously ships PENDING rows to Kafka.
 */
@Entity
@Table(name = "outbox_event")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    @GeneratedValue
    private UUID id;

    private String aggregateId;   // sagaId / orderId
    private String eventType;     // e.g. OrderCreated, InventoryReserved...
    private String topic;         // target Kafka topic
    private String partitionKey;  // optional partition key (defaults to aggregateId)

    @Lob
    @Column(columnDefinition = "TEXT")
    private String payload;       // pre-serialized JSON of SagaEvent

    @Enumerated(EnumType.STRING)
    private OutboxStatus status;

    private Instant createdAt;
    private Instant publishedAt;

    @PrePersist
    public void prePersist() {
        if (createdAt == null) createdAt = Instant.now();
        if (status == null) status = OutboxStatus.PENDING;
        if (partitionKey == null) partitionKey = aggregateId;
    }
}
