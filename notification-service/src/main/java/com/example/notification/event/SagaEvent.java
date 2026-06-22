package com.example.notification.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SagaEvent implements Serializable {

    private String eventId;
    private String sagaId;
    private String eventType;
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
