package com.example.order.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Generic envelope used for every saga event across every service.
 * `payload` carries the order's full context forward at each hop so any
 * downstream service has everything it needs (orderId, productId, amount,
 * customerId, plus the simulate* test flags) without calling back upstream.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaEvent implements Serializable {

    private String eventId;
    private String sagaId;        // == orderId, ties the whole saga together
    private String eventType;     // OrderCreated, InventoryReserved, PaymentFailed, ...
    private String sourceService;
    private Instant timestamp;

    @Builder.Default
    private Map<String, Object> payload = new HashMap<>();

    public static SagaEvent create(String sagaId, String eventType, String sourceService, Map<String, Object> payload) {
        return SagaEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .sagaId(sagaId)
                .eventType(eventType)
                .sourceService(sourceService)
                .timestamp(Instant.now())
                .payload(payload)
                .build();
    }
}
