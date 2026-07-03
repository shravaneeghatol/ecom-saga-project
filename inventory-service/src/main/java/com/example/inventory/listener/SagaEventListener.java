//package com.example.inventory.listener;
//
//import com.example.inventory.event.SagaEvent;
//import com.example.inventory.service.InventoryService;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.dao.DataAccessException;
//import org.springframework.kafka.annotation.DltHandler;
//import org.springframework.kafka.annotation.KafkaListener;
//import org.springframework.kafka.annotation.RetryableTopic;
//import org.springframework.kafka.retrytopic.DltStrategy;
//import org.springframework.retry.annotation.Backoff;
//import org.springframework.stereotype.Component;
//
//import java.util.Map;
//import java.util.concurrent.ConcurrentHashMap;
//
//@Component
//@RequiredArgsConstructor
//@Slf4j
//public class SagaEventListener {
//
//    private final InventoryService inventoryService;
//    private final ObjectMapper objectMapper;
//
//    // Idempotency cache for processed messages
//    private final Map<String, Boolean> processedMessages = new ConcurrentHashMap<>();
//
//    @RetryableTopic(
//            attempts = "4",
//            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 15000),
//            retryTopicSuffix = "-retry",
//            dltTopicSuffix = "-dlt",
//            dltStrategy = DltStrategy.FAIL_ON_ERROR,
//            include = { RuntimeException.class, DataAccessException.class }
//    )
//    @KafkaListener(topics = "order.events",
//            groupId = "inventory-service-group",
//            containerFactory = "kafkaListenerContainerFactory")
//    public void onOrderCreated(SagaEvent event) {
//        if (!"OrderCreated".equals(event.getEventType())) {
//            log.debug("Ignoring event type: {}", event.getEventType());
//            return;
//        }
//
//        String orderId = event.getSagaId();
//        String eventId = event.getEventId();
//
//        // Idempotency check
//        String messageKey = "order-created:" + orderId + ":" + eventId;
//        if (processedMessages.putIfAbsent(messageKey, Boolean.TRUE) != null) {
//            log.info("Duplicate OrderCreated event for order {} (eventId: {}), skipping", orderId, eventId);
//            return;
//        }
//
//        try {
//            log.info("Processing OrderCreated event for order: {}", orderId);
//
//            Map<String, Object> payload = event.getPayload();
//
//            // Validate payload
//            if (!payload.containsKey("productId") || !payload.containsKey("quantity")) {
//                log.error("Invalid payload for order {}: missing productId or quantity", orderId);
//                inventoryService.publishFailure(orderId, payload, "Invalid payload: missing required fields");
//                processedMessages.remove(messageKey);
//                return;
//            }
//
//            boolean simulateBusinessFailure = Boolean.TRUE.equals(
//                    payload.getOrDefault("simulateInventoryFailure", false)
//            );
//
//            inventoryService.reserve(orderId, payload, simulateBusinessFailure);
//
//        } catch (Exception e) {
//            log.error("Error processing OrderCreated for order {}: {}", orderId, e.getMessage(), e);
//            processedMessages.remove(messageKey);
//            throw e;
//        }
//
//        // Clean up after successful processing
//        processedMessages.remove(messageKey);
//    }
//
//    @DltHandler
//    public void handleOrderDlt(SagaEvent event) {
//        String orderId = event.getSagaId();
//        log.error("[DLT] order.events exhausted retries for order {}", orderId);
//
//        // DLT processing should be idempotent
//        String dltKey = "dlt:" + orderId + ":" + event.getEventId();
//        if (processedMessages.putIfAbsent(dltKey, Boolean.TRUE) != null) {
//            log.info("Duplicate DLT event for order {}, skipping", orderId);
//            return;
//        }
//
//        try {
//            inventoryService.publishFailure(
//                    orderId,
//                    event.getPayload(),
//                    "Inventory processing failed after max retries (DLT)"
//            );
//        } finally {
//            processedMessages.remove(dltKey);
//        }
//    }
//
//    @KafkaListener(topics = "payment.events", groupId = "inventory-service-compensation-group")
//    public void onPaymentEvent(SagaEvent event) {
//        if (!"PaymentFailed".equals(event.getEventType())) {
//            return;
//        }
//
//        String orderId = event.getSagaId();
//        String eventId = event.getEventId();
//
//        // Idempotency check
//        String key = "payment-failed:" + orderId + ":" + eventId;
//        if (processedMessages.putIfAbsent(key, Boolean.TRUE) != null) {
//            log.info("Duplicate PaymentFailed event for order {}, skipping", orderId);
//            return;
//        }
//
//        try {
//            String reason = String.valueOf(
//                    event.getPayload().getOrDefault("reason", "payment failed"));
//            log.warn("PaymentFailed for order {} — triggering compensation", orderId);
//            inventoryService.releaseReservation(orderId, reason);
//        } finally {
//            processedMessages.remove(key);
//        }
//    }
//
//    @KafkaListener(topics = "notification.events", groupId = "inventory-service-compensation-group")
//    public void onNotificationEvent(SagaEvent event) {
//        if (!"NotificationFailed".equals(event.getEventType())) {
//            return;
//        }
//
//        String orderId = event.getSagaId();
//        String eventId = event.getEventId();
//
//        String key = "notification-failed:" + orderId + ":" + eventId;
//        if (processedMessages.putIfAbsent(key, Boolean.TRUE) != null) {
//            log.info("Duplicate NotificationFailed event for order {}, skipping", orderId);
//            return;
//        }
//
//        try {
//            log.warn("NotificationFailed for order {} — triggering compensation", orderId);
//            inventoryService.releaseReservation(orderId, "Compensating due to notification failure");
//        } finally {
//            processedMessages.remove(key);
//        }
//    }
//
//    @KafkaListener(topics = "inventory.events-dlt",
//            groupId = "inventory-service-dlt-group",
//            containerFactory = "kafkaListenerContainerFactory")
//    public void onInventoryEventsDlt(String rawPayload) {
//        try {
//            @SuppressWarnings("unchecked")
//            Map<String, Object> dltEvent = objectMapper.readValue(rawPayload, Map.class);
//
//            String orderId = (String) dltEvent.get("aggregateId");
//            String errorReason = (String) dltEvent.get("errorReason");
//
//            if (orderId == null) {
//                log.error("[DLT-CONSUMER] Invalid DLT message: missing aggregateId");
//                return;
//            }
//
//            // Idempotency check
//            String key = "dlt-consumer:" + orderId;
//            if (processedMessages.putIfAbsent(key, Boolean.TRUE) != null) {
//                log.info("Duplicate DLT consumer event for order {}, skipping", orderId);
//                return;
//            }
//
//            log.error("[DLT-CONSUMER] inventory.events-dlt received for orderId={}, reason={}",
//                    orderId, errorReason);
//
//            // Release the stock reservation — payment never happened
//            inventoryService.releaseReservation(orderId,
//                    "Payment-service unreachable after max retries: " + errorReason);
//
//            processedMessages.remove(key);
//
//        } catch (Exception e) {
//            log.error("[DLT-CONSUMER] failed to process DLT message: {}", e.getMessage());
//        }
//    }
//}



package com.example.inventory.listener;

import com.example.inventory.event.SagaEvent;
import com.example.inventory.service.InventoryService;
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

    private final InventoryService inventoryService;
    private final ObjectMapper objectMapper;

    // NOTE (production limitation): this is an in-memory, per-instance idempotency
    // cache. It does NOT protect against duplicate delivery across pod restarts,
    // rebalances, or multiple replicas of inventory-service — each instance has
    // its own cache. For real exactly-once semantics you need a durable dedup
    // store (e.g. a `processed_events(event_id UNIQUE)` table checked/inserted
    // in the same transaction as the business write, or a Kafka Streams state
    // store). Left as-is here since it's out of scope for this change, but flag
    // it before this goes to production with >1 replica.
    private final Map<String, Boolean> processedMessages = new ConcurrentHashMap<>();

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 15000),
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = { RuntimeException.class, DataAccessException.class }
    )
    @KafkaListener(topics = "order.events",
            groupId = "inventory-service-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onOrderCreated(SagaEvent event) {
        if (!"OrderCreated".equals(event.getEventType())) {
            log.debug("Ignoring event type: {}", event.getEventType());
            return;
        }

        String orderId = event.getSagaId();
        String eventId = event.getEventId();

        // Idempotency check
        String messageKey = "order-created:" + orderId + ":" + eventId;
        if (processedMessages.putIfAbsent(messageKey, Boolean.TRUE) != null) {
            log.info("Duplicate OrderCreated event for order {} (eventId: {}), skipping", orderId, eventId);
            return;
        }

        try {
            log.info("Processing OrderCreated event for order: {}", orderId);

            Map<String, Object> payload = event.getPayload();

            // Validate payload
            if (!payload.containsKey("productId") || !payload.containsKey("quantity")) {
                log.error("Invalid payload for order {}: missing productId or quantity", orderId);
                inventoryService.publishFailure(orderId, payload, "Invalid payload: missing required fields");
                processedMessages.remove(messageKey);
                return;
            }

            inventoryService.reserve(orderId, payload);

        } catch (Exception e) {
            log.error("Error processing OrderCreated for order {}: {}", orderId, e.getMessage(), e);
            processedMessages.remove(messageKey);
            throw e;
        }

        // Clean up after successful processing
        processedMessages.remove(messageKey);
    }

    @DltHandler
    public void handleOrderDlt(SagaEvent event) {
        String orderId = event.getSagaId();
        log.error("[DLT] order.events exhausted retries for order {}", orderId);

        // DLT processing should be idempotent
        String dltKey = "dlt:" + orderId + ":" + event.getEventId();
        if (processedMessages.putIfAbsent(dltKey, Boolean.TRUE) != null) {
            log.info("Duplicate DLT event for order {}, skipping", orderId);
            return;
        }

        try {
            inventoryService.publishFailure(
                    orderId,
                    event.getPayload(),
                    "Inventory processing failed after max retries (DLT)"
            );
        } finally {
            processedMessages.remove(dltKey);
        }
    }

    @KafkaListener(topics = "payment.events", groupId = "inventory-service-compensation-group")
    public void onPaymentEvent(SagaEvent event) {
        if (!"PaymentFailed".equals(event.getEventType())) {
            return;
        }

        String orderId = event.getSagaId();
        String eventId = event.getEventId();

        // Idempotency check
        String key = "payment-failed:" + orderId + ":" + eventId;
        if (processedMessages.putIfAbsent(key, Boolean.TRUE) != null) {
            log.info("Duplicate PaymentFailed event for order {}, skipping", orderId);
            return;
        }

        try {
            String reason = String.valueOf(
                    event.getPayload().getOrDefault("reason", "payment failed"));
            log.warn("PaymentFailed for order {} — triggering compensation", orderId);
            inventoryService.releaseReservation(orderId, reason);
        } finally {
            processedMessages.remove(key);
        }
    }

    @KafkaListener(topics = "notification.events", groupId = "inventory-service-compensation-group")
    public void onNotificationEvent(SagaEvent event) {
        if (!"NotificationFailed".equals(event.getEventType())) {
            return;
        }

        String orderId = event.getSagaId();
        String eventId = event.getEventId();

        String key = "notification-failed:" + orderId + ":" + eventId;
        if (processedMessages.putIfAbsent(key, Boolean.TRUE) != null) {
            log.info("Duplicate NotificationFailed event for order {}, skipping", orderId);
            return;
        }

        try {
            log.warn("NotificationFailed for order {} — triggering compensation", orderId);
            inventoryService.releaseReservation(orderId, "Compensating due to notification failure");
        } finally {
            processedMessages.remove(key);
        }
    }

    @KafkaListener(topics = "inventory.events-dlt",
            groupId = "inventory-service-dlt-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onInventoryEventsDlt(String rawPayload) {
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
            String key = "dlt-consumer:" + orderId;
            if (processedMessages.putIfAbsent(key, Boolean.TRUE) != null) {
                log.info("Duplicate DLT consumer event for order {}, skipping", orderId);
                return;
            }

            log.error("[DLT-CONSUMER] inventory.events-dlt received for orderId={}, reason={}",
                    orderId, errorReason);

            // Release the stock reservation — payment never happened
            inventoryService.releaseReservation(orderId,
                    "Payment-service unreachable after max retries: " + errorReason);

            processedMessages.remove(key);

        } catch (Exception e) {
            log.error("[DLT-CONSUMER] failed to process DLT message: {}", e.getMessage());
        }
    }
}