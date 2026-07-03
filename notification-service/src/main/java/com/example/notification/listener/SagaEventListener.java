package com.example.notification.listener;

import com.example.notification.event.SagaEvent;
import com.example.notification.service.NotificationService;
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

    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;

    // Idempotency cache for processed messages
    private final Map<String, Boolean> processedMessages = new ConcurrentHashMap<>();

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 15000),
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = { RuntimeException.class, DataAccessException.class }
    )
    @KafkaListener(topics = "payment.events",
            groupId = "notification-service-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onPaymentCompleted(SagaEvent event) {
        if (!"PaymentCompleted".equals(event.getEventType())) {
            log.debug("Ignoring event type: {}", event.getEventType());
            return;
        }

        String orderId = event.getSagaId();
        String eventId = event.getEventId();

        // Idempotency check
        String messageKey = "payment-completed:" + orderId + ":" + eventId;
        if (processedMessages.putIfAbsent(messageKey, Boolean.TRUE) != null) {
            log.info("Duplicate PaymentCompleted event for order {} (eventId: {}), skipping", orderId, eventId);
            return;
        }

        try {
            log.info("Processing PaymentCompleted event for order: {}", orderId);

            Map<String, Object> payload = event.getPayload();

            // Validate payload
            if (!payload.containsKey("customerId")) {
                log.error("Invalid payload for order {}: missing customerId", orderId);
                notificationService.publishFailure(orderId, payload, "Invalid payload: missing customerId");
                processedMessages.remove(messageKey);
                return;
            }

            // Check for business failure flag
            boolean businessFailure = Boolean.TRUE.equals(
                    payload.getOrDefault("businessFailure", false)
            );

            if (businessFailure) {
                log.warn("[BUSINESS-FAILURE] order {} → failure topic", orderId);
                notificationService.publishFailure(orderId, payload, "Business failure in notification-service");
                processedMessages.remove(messageKey);
                return;
            }

            notificationService.sendNotification(orderId, payload);

        } catch (Exception e) {
            log.error("Error processing PaymentCompleted for order {}: {}", orderId, e.getMessage(), e);
            processedMessages.remove(messageKey);
            throw e;
        }

        // Clean up after successful processing
        processedMessages.remove(messageKey);
    }

    @DltHandler
    public void handlePaymentDlt(SagaEvent event) {
        String orderId = event.getSagaId();
        log.error("[DLT] payment.events exhausted retries for order {}", orderId);

        // DLT processing should be idempotent
        String dltKey = "dlt:" + orderId + ":" + event.getEventId();
        if (processedMessages.putIfAbsent(dltKey, Boolean.TRUE) != null) {
            log.info("Duplicate DLT event for order {}, skipping", orderId);
            return;
        }

        try {
            notificationService.publishFailure(
                    orderId,
                    event.getPayload(),
                    "Notification failed after max retries (DLT)"
            );
        } finally {
            processedMessages.remove(dltKey);
        }
    }

    /**
     * DLT listener for payment.events-dlt
     * Handles cases where payment service exhausted retries
     */
    @KafkaListener(topics = "payment.events-dlt",
            groupId = "notification-service-dlt-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onPaymentDlt(String rawPayload) {
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
            String key = "dlt-payment:" + orderId;
            if (processedMessages.putIfAbsent(key, Boolean.TRUE) != null) {
                log.info("Duplicate DLT payment event for order {}, skipping", orderId);
                return;
            }

            log.error("[DLT-CONSUMER] payment.events-dlt received for orderId={}, reason={}",
                    orderId, errorReason);

            // Since payment failed, we don't need to process notification
            // Just log and don't send notification
            log.warn("Payment failed for order {}, notification not sent", orderId);

            // Optionally send a failure notification
            Map<String, Object> payload = new ConcurrentHashMap<>();
            payload.put("orderId", orderId);
            payload.put("reason", "Payment service unavailable: " + errorReason);

            notificationService.publishFailure(orderId, payload, "Payment service unavailable");

            processedMessages.remove(key);

        } catch (Exception e) {
            log.error("[DLT-CONSUMER] failed to process payment DLT message: {}", e.getMessage());
        }
    }
}