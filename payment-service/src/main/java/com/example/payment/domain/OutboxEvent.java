package com.example.payment.domain;

import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

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

    private String aggregateId;
    private String eventType;
    private String topic;
    private String partitionKey;

    @Lob
    @Column(columnDefinition = "TEXT")
    private String payload;

    @Enumerated(EnumType.STRING)
    private OutboxStatus status;

    private Instant createdAt;
    private Instant publishedAt;

    // ── Retry / DLT fields ──────────────────────────────────────────────────
    /** How many times this event has already failed to publish. Starts at 0. */
    @Builder.Default
    @Column(nullable = false)
    private int retryCount = 0;

    /**
     * Maximum allowed publish attempts before this event is considered dead.
     * Default = 3  →  4 total attempts (1 initial + 3 retries).
     */
    @Builder.Default
    @Column(nullable = false)
    private int maxRetries = 3;

    /** Set when status transitions to FAILED (after exhausting retries). */
    private Instant failedAt;
    // ────────────────────────────────────────────────────────────────────────

    @PrePersist
    public void prePersist() {
        if (createdAt == null) createdAt = Instant.now();
        if (status == null) status = OutboxStatus.PENDING;
        if (partitionKey == null) partitionKey = aggregateId;
    }
}