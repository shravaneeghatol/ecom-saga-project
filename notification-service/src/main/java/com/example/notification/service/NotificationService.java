package com.example.notification.service;

import com.example.notification.config.KafkaTopicConfig;
import com.example.notification.domain.*;
import com.example.notification.event.SagaEvent;
import com.example.notification.repository.NotificationRepository;
import com.example.notification.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final NotificationRepository notificationRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    /**
     * FINAL saga step: notify the customer. This is the row that, once present
     * in EVERY service's table, confirms the whole saga succeeded end-to-end.
     */
    @Transactional
    public void sendNotification(String orderId, Map<String, Object> incomingPayload) {
        String customerId = (String) incomingPayload.get("customerId");

        Notification n = Notification.builder()
                .orderId(orderId)
                .customerId(customerId)
                .message("Your order " + orderId + " has been placed successfully!")
                .status(NotificationStatus.SENT)
                .build();
        n = notificationRepository.save(n);

        Map<String, Object> payload = new HashMap<>(incomingPayload);
        payload.put("notificationId", n.getId().toString());
        saveOutbox(orderId, "NotificationSent", KafkaTopicConfig.NOTIFICATION_EVENTS_TOPIC, payload);
        log.info("Notification SENT for order {}", orderId);
    }

    public void publishFailure(String orderId, Map<String, Object> incomingPayload, String reason) {
        Notification n = Notification.builder()
                .orderId(orderId)
                .customerId((String) incomingPayload.get("customerId"))
                .message("Failed to notify customer for order " + orderId)
                .status(NotificationStatus.FAILED)
                .build();
        notificationRepository.save(n);

        Map<String, Object> payload = new HashMap<>(incomingPayload);
        payload.put("reason", reason);
        saveOutbox(orderId, "NotificationFailed", KafkaTopicConfig.NOTIFICATION_EVENTS_TOPIC, payload);
    }

    private void saveOutbox(String aggregateId, String eventType, String topic, Map<String, Object> payload) {
        try {
            SagaEvent event = SagaEvent.create(aggregateId, eventType, "notification-service", payload);
            String json = objectMapper.writeValueAsString(event);
            OutboxEvent outbox = OutboxEvent.builder()
                    .aggregateId(aggregateId).eventType(eventType).topic(topic).payload(json).build();
            outboxEventRepository.save(outbox);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize saga event", e);
        }
    }
}
