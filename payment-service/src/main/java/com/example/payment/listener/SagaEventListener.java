package com.example.payment.listener;

import com.example.payment.event.SagaEvent;
import com.example.payment.service.PaymentService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
@RequiredArgsConstructor
@Slf4j
public class SagaEventListener {

    private final PaymentService paymentService;
    private final ObjectMapper objectMapper;

    // Idempotency cache for processed messages
    private final Map<String, Boolean> processedMessages = new ConcurrentHashMap<>();

    /**
     * PRIMARY listener: inventory.events
     * Creates: inventory.events-retry-0, -retry-1, -retry-2, -dlt
     */
    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 15000),
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = { RuntimeException.class, DataAccessException.class }
    )
    @KafkaListener(topics = "inventory.events",
            groupId = "payment-service-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onInventoryReserved(SagaEvent event) {
        if (!"InventoryReserved".equals(event.getEventType())) {
            log.debug("Ignoring event type: {}", event.getEventType());
            return;
        }

        String orderId = event.getSagaId();
        String eventId = event.getEventId();

        // Idempotency check
        String messageKey = "inventory-reserved:" + orderId + ":" + eventId;
        if (processedMessages.putIfAbsent(messageKey, Boolean.TRUE) != null) {
            log.info("Duplicate InventoryReserved event for order {} (eventId: {}), skipping", orderId, eventId);
            return;
        }

        try {
            log.info("Processing InventoryReserved event for order: {}", orderId);

            Map<String, Object> payload = event.getPayload();

            // Validate payload
            if (!payload.containsKey("amount") && !payload.containsKey("totalAmount")) {
                log.error("Invalid payload for order {}: missing amount", orderId);
                paymentService.publishFailure(orderId, payload, "Invalid payload: missing amount");
                processedMessages.remove(messageKey);
                return;
            }

            // Check for business failure flag (only from actual business logic, not simulation)
            boolean businessFailure = Boolean.TRUE.equals(
                    payload.getOrDefault("businessFailure", false)
            );

            paymentService.processPayment(orderId, payload, businessFailure);

        } catch (Exception e) {
            log.error("Error processing InventoryReserved for order {}: {}", orderId, e.getMessage(), e);
            processedMessages.remove(messageKey);
            throw e;
        }

        // Clean up after successful processing
        processedMessages.remove(messageKey);
    }

    @DltHandler
    public void handleInventoryDlt(SagaEvent event) {
        String orderId = event.getSagaId();
        log.error("[DLT] inventory.events exhausted retries for order {}", orderId);

        // DLT processing should be idempotent
        String dltKey = "dlt:" + orderId + ":" + event.getEventId();
        if (processedMessages.putIfAbsent(dltKey, Boolean.TRUE) != null) {
            log.info("Duplicate DLT event for order {}, skipping", orderId);
            return;
        }

        try {
            paymentService.publishFailure(
                    orderId,
                    event.getPayload(),
                    "Payment processing failed after max retries (DLT)"
            );
        } finally {
            processedMessages.remove(dltKey);
        }
    }

    /**
     * Compensation listener: notification.events (refund if notification ultimately fails)
     */
    @KafkaListener(topics = "notification.events", groupId = "payment-service-compensation-group")
    public void onNotificationEvent(SagaEvent event) {
        if (!"NotificationFailed".equals(event.getEventType())) {
            return;
        }

        String orderId = event.getSagaId();
        String eventId = event.getEventId();

        // Idempotency check
        String key = "notification-failed:" + orderId + ":" + eventId;
        if (processedMessages.putIfAbsent(key, Boolean.TRUE) != null) {
            log.info("Duplicate NotificationFailed event for order {}, skipping", orderId);
            return;
        }

        try {
            log.warn("NotificationFailed for order {} — triggering payment refund", orderId);
            paymentService.refund(orderId, "Compensating due to notification failure");
        } finally {
            processedMessages.remove(key);
        }
    }

    /**
     * DLT listener for inventory.events-dlt
     * Handles cases where inventory service exhausted retries
     */
    @KafkaListener(topics = "inventory.events-dlt",
            groupId = "payment-service-dlt-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onInventoryDlt(String rawPayload) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> dltEvent = objectMapper.readValue(rawPayload, Map.class);

            String orderId = (String) dltEvent.get("aggregateId");
            String errorReason = (String) dltEvent.get("errorReason");

            if (orderId == null) {
                log.error("[DLT-CONSUMER] Invalid DLT message: missing aggregateId");
                return;
            }

            // Idempotency check
            String key = "dlt-inventory:" + orderId;
            if (processedMessages.putIfAbsent(key, Boolean.TRUE) != null) {
                log.info("Duplicate DLT inventory event for order {}, skipping", orderId);
                return;
            }

            log.error("[DLT-CONSUMER] inventory.events-dlt received for orderId={}, reason={}",
                    orderId, errorReason);

            // Since inventory failed, we don't need to process payment
            // Just notify order service about failure
            Map<String, Object> payload = new ConcurrentHashMap<>();
            payload.put("orderId", orderId);
            payload.put("reason", "Inventory service unavailable: " + errorReason);

            paymentService.publishFailure(orderId, payload, "Inventory service unavailable");

            processedMessages.remove(key);

        } catch (Exception e) {
            log.error("[DLT-CONSUMER] failed to process inventory DLT message: {}", e.getMessage());
        }
    }
}