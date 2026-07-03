//package com.example.inventory.service;
//
//import com.example.inventory.client.NotificationServiceClient;
//import com.example.inventory.client.PaymentServiceClient;
//import com.example.inventory.config.KafkaTopicConfig;
//import com.example.inventory.domain.*;
//import com.example.inventory.dto.NotificationRequest;
//import com.example.inventory.dto.PaymentRequest;
//import com.example.inventory.dto.PaymentResponse;
//import com.example.inventory.event.SagaEvent;
//import com.example.inventory.repository.InventoryItemRepository;
//import com.example.inventory.repository.InventoryReservationRepository;
//import com.example.inventory.repository.OutboxEventRepository;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
//import lombok.RequiredArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.springframework.dao.DataAccessException;
//import org.springframework.dao.OptimisticLockingFailureException;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.retry.annotation.Backoff;
//import org.springframework.retry.annotation.Retryable;
//import org.springframework.stereotype.Service;
//import org.springframework.transaction.annotation.Transactional;
//
//import java.time.Instant;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.Optional;
//import java.util.UUID;
//import java.util.concurrent.ConcurrentHashMap;
//
//import static com.example.inventory.config.CircuitBreakerConfig.*;
//
//@Service
//@RequiredArgsConstructor
//@Slf4j
//public class InventoryService {
//
//    private final InventoryItemRepository inventoryItemRepository;
//    private final InventoryReservationRepository reservationRepository;
//    private final OutboxEventRepository outboxEventRepository;
//    private final ObjectMapper objectMapper;
//    private final KafkaTemplate<String, String> kafkaTemplate;
//    private final PaymentServiceClient paymentServiceClient;
//    private final NotificationServiceClient notificationServiceClient;
//
//    private final Map<String, Boolean> processedEventCache = new ConcurrentHashMap<>();
//
//    @Transactional
//    @Retryable(
//            value = {OptimisticLockingFailureException.class, DataAccessException.class},
//            maxAttempts = 3,
//            backoff = @Backoff(delay = 100, multiplier = 2.0)
//    )
//    public void reserve(String orderId, Map<String, Object> incomingPayload, boolean simulateFailure) {
//        String eventKey = "reserve:" + orderId;
//        if (processedEventCache.putIfAbsent(eventKey, Boolean.TRUE) != null) {
//            log.info("Duplicate reserve event detected for order {}, skipping", orderId);
//            return;
//        }
//
//        Optional<InventoryReservation> existingReservation =
//                reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatus.RESERVED);
//
//        if (existingReservation.isPresent()) {
//            log.info("Reservation already exists for order {}, skipping", orderId);
//            processedEventCache.remove(eventKey);
//            return;
//        }
//
//        try {
//            String productId = (String) incomingPayload.get("productId");
//            Integer qty = (Integer) incomingPayload.get("quantity");
//
//            if (productId == null || qty == null) {
//                log.error("Invalid payload for order {}: missing productId or quantity", orderId);
//                // ONLY send to failure topic - DO NOT send to inventory.events
//                sendFailureToTopic(orderId, incomingPayload, "Invalid order payload: missing productId or quantity");
//                processedEventCache.remove(eventKey);
//                return;
//            }
//
//            Optional<InventoryItem> itemOpt = inventoryItemRepository.findByProductId(productId);
//
//            // BUSINESS FAILURE - insufficient stock
//            if (itemOpt.isEmpty() || itemOpt.get().getAvailableQty() < qty) {
//                InventoryReservation r = InventoryReservation.builder()
//                        .orderId(orderId)
//                        .productId(productId)
//                        .quantity(qty)
//                        .status(ReservationStatus.FAILED)
//                        .build();
//                reservationRepository.save(r);
//
//                String reason = itemOpt.isEmpty()
//                        ? "Product not found: " + productId
//                        : "Insufficient stock. Available: " + itemOpt.get().getAvailableQty() + ", Requested: " + qty;
//
//                // ONLY send to failure topic - DO NOT send to inventory.events
//                sendFailureToTopic(orderId, incomingPayload, reason);
//                log.warn("Reservation FAILED for order {} (product {}): {}", orderId, productId, reason);
//                processedEventCache.remove(eventKey);
//                return;
//            }
//
//            // SUCCESS - Reserve inventory
//            InventoryItem item = itemOpt.get();
//            item.setAvailableQty(item.getAvailableQty() - qty);
//            item.setReservedQty(item.getReservedQty() + qty);
//            inventoryItemRepository.save(item);
//
//            InventoryReservation r = InventoryReservation.builder()
//                    .orderId(orderId)
//                    .productId(productId)
//                    .quantity(qty)
//                    .status(ReservationStatus.RESERVED)
//                    .build();
//            r = reservationRepository.save(r);
//
//            Map<String, Object> payload = new HashMap<>(incomingPayload);
//            payload.put("reservationId", r.getId().toString());
//            payload.put("reservedAt", Instant.now().toString());
//
//            // SUCCESS - ONLY send to inventory.events (NOT failure topic)
//            saveOutbox(orderId, "InventoryReserved", KafkaTopicConfig.INVENTORY_EVENTS_TOPIC, payload);
//            log.info("Reservation RESERVED for order {} (product {}, qty {})", orderId, productId, qty);
//
//            notifyPaymentService(orderId, payload);
//
//        } catch (Exception e) {
//            log.error("Error reserving inventory for order {}: {}", orderId, e.getMessage(), e);
//            processedEventCache.remove(eventKey);
//            throw e;
//        }
//
//        processedEventCache.remove(eventKey);
//    }
//
//    public void publishFailure(String orderId, Map<String, Object> payload, String reason) {
//        sendFailureToTopic(orderId, payload, reason);
//    }
//
//    @CircuitBreaker(name = PAYMENT_SERVICE_CB)
//    @Retryable(
//            value = {Exception.class},
//            maxAttempts = 3,
//            backoff = @Backoff(delay = 1000, multiplier = 2.0)
//    )
//    private void notifyPaymentService(String orderId, Map<String, Object> incomingPayload) {
//        try {
//            Double amount = (Double) incomingPayload.getOrDefault("totalAmount", 0.0);
//            if (amount == 0.0) {
//                amount = (Double) incomingPayload.getOrDefault("amount", 0.0);
//            }
//
//            PaymentRequest paymentRequest = PaymentRequest.builder()
//                    .orderId(orderId)
//                    .amount(amount)
//                    .customerId((String) incomingPayload.get("customerId"))
//                    .build();
//
//            PaymentResponse response = paymentServiceClient.processPayment(paymentRequest);
//
//            if (response != null && !response.isSuccess()) {
//                log.error("Payment service rejected payment for order {}: {}", orderId, response.getMessage());
//                Map<String, Object> failurePayload = new HashMap<>(incomingPayload);
//                failurePayload.put("paymentRejectionReason", response.getMessage());
//                // ONLY send to failure topic
//                sendFailureToTopic(orderId, failurePayload, "Payment rejected: " + response.getMessage());
//                releaseReservation(orderId, "Payment rejected: " + response.getMessage());
//            } else {
//                log.info("Payment service notified successfully for order: {}", orderId);
//            }
//        } catch (Exception e) {
//            log.error("Failed to notify payment service for order {}: {}", orderId, e.getMessage());
//            throw e;
//        }
//    }
//
//    @Transactional
//    @Retryable(
//            value = {OptimisticLockingFailureException.class},
//            maxAttempts = 3,
//            backoff = @Backoff(delay = 100, multiplier = 2.0)
//    )
//    public void releaseReservation(String orderId, String reason) {
//        String eventKey = "release:" + orderId;
//        if (processedEventCache.putIfAbsent(eventKey, Boolean.TRUE) != null) {
//            log.info("Duplicate release event detected for order {}, skipping", orderId);
//            return;
//        }
//
//        try {
//            Optional<InventoryReservation> reservationOpt =
//                    reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatus.RESERVED);
//
//            if (reservationOpt.isEmpty()) {
//                log.info("No active reservation found for order {}, already released or not reserved", orderId);
//                processedEventCache.remove(eventKey);
//                return;
//            }
//
//            InventoryReservation r = reservationOpt.get();
//            Optional<InventoryItem> itemOpt = inventoryItemRepository.findByProductId(r.getProductId());
//
//            if (itemOpt.isPresent()) {
//                InventoryItem item = itemOpt.get();
//                item.setAvailableQty(item.getAvailableQty() + r.getQuantity());
//                item.setReservedQty(Math.max(0, item.getReservedQty() - r.getQuantity()));
//                inventoryItemRepository.save(item);
//            }
//
//            r.setStatus(ReservationStatus.RELEASED);
//            reservationRepository.save(r);
//
//            Map<String, Object> payload = new HashMap<>();
//            payload.put("orderId", orderId);
//            payload.put("reservationId", r.getId().toString());
//            payload.put("reason", reason);
//            payload.put("releasedAt", Instant.now().toString());
//
//            // SUCCESS - ONLY send to inventory.events
//            saveOutbox(orderId, "InventoryReleased", KafkaTopicConfig.INVENTORY_EVENTS_TOPIC, payload);
//            log.warn("Reservation RELEASED for order {} (compensating). Reason: {}", orderId, reason);
//
//            notifyRelease(orderId, reason);
//
//        } catch (Exception e) {
//            log.error("Error releasing reservation for order {}: {}", orderId, e.getMessage(), e);
//            processedEventCache.remove(eventKey);
//            throw e;
//        }
//
//        processedEventCache.remove(eventKey);
//    }
//
//    @CircuitBreaker(name = NOTIFICATION_SERVICE_CB)
//    private void notifyRelease(String orderId, String reason) {
//        try {
//            NotificationRequest notificationRequest = NotificationRequest.builder()
//                    .orderId(orderId)
//                    .customerId("customer-" + orderId)
//                    .message("Inventory released for order " + orderId + " due to: " + reason)
//                    .type("INVENTORY_RELEASED")
//                    .build();
//
//            notificationServiceClient.sendNotification(notificationRequest);
//            log.info("Release notification sent for order: {}", orderId);
//        } catch (Exception e) {
//            log.error("Failed to send release notification for order {}: {}", orderId, e.getMessage());
//        }
//    }
//
//    /**
//     * ONLY sends to failure topic - NEVER to inventory.events
//     */
//    private void sendFailureToTopic(String orderId, Map<String, Object> incomingPayload, String reason) {
//        try {
//            Map<String, Object> failureEvent = new HashMap<>();
//            failureEvent.put("orderId", orderId);
//            failureEvent.put("service", "inventory-service");
//            failureEvent.put("failureType", "BUSINESS");
//            failureEvent.put("reason", reason);
//            failureEvent.put("payload", incomingPayload);
//            failureEvent.put("timestamp", Instant.now().toString());
//
//            String failureJson = objectMapper.writeValueAsString(failureEvent);
//            kafkaTemplate.send(KafkaTopicConfig.INVENTORY_EVENTS_FAILURE_TOPIC, orderId, failureJson);
//            log.warn("[FAILURE-TOPIC] Business failure - Order {} → inventory.events-failure | reason: {}", orderId, reason);
//        } catch (Exception e) {
//            log.error("[FAILURE-TOPIC] Failed to send to failure topic for order {}: {}", orderId, e.getMessage());
//        }
//        // ⚠️ IMPORTANT: Do NOT call saveOutbox() here!
//        // This method ONLY sends to the failure topic
//    }
//
//    /**
//     * Saves outbox event - ONLY for SUCCESSFUL events
//     * This goes to inventory.events topic
//     */
//    private void saveOutbox(String aggregateId, String eventType, String topic, Map<String, Object> payload) {
//        try {
//            String eventKey = "outbox:" + aggregateId + ":" + eventType;
//            if (processedEventCache.putIfAbsent(eventKey, Boolean.TRUE) != null) {
//                log.debug("Duplicate outbox entry detected for {}-{}, skipping", aggregateId, eventType);
//                processedEventCache.remove(eventKey);
//                return;
//            }
//
//            SagaEvent event = SagaEvent.create(aggregateId, eventType, "inventory-service", payload);
//            String json = objectMapper.writeValueAsString(event);
//
//            OutboxEvent outbox = OutboxEvent.builder()
//                    .aggregateId(aggregateId)
//                    .eventType(eventType)
//                    .topic(topic)
//                    .payload(json)
//                    .partitionKey(aggregateId)
//                    .build();
//
//            outboxEventRepository.save(outbox);
//            log.debug("Outbox event saved: {} for order {}", eventType, aggregateId);
//
//            processedEventCache.remove(eventKey);
//        } catch (Exception e) {
//            log.error("Failed to save outbox event for order {}: {}", aggregateId, e.getMessage(), e);
//            throw new RuntimeException("Failed to serialize saga event", e);
//        }
//    }
//
//    public void cleanupCache() {
//        processedEventCache.clear();
//        log.debug("Cleaned up processed event cache");
//    }
//}


package com.example.inventory.service;

import com.example.inventory.client.NotificationServiceClient;
import com.example.inventory.config.KafkaTopicConfig;
import com.example.inventory.domain.*;
import com.example.inventory.dto.NotificationRequest;
import com.example.inventory.event.SagaEvent;
import com.example.inventory.repository.InventoryItemRepository;
import com.example.inventory.repository.InventoryReservationRepository;
import com.example.inventory.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static com.example.inventory.config.CircuitBreakerConfig.NOTIFICATION_SERVICE_CB;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryService {

    private final InventoryItemRepository inventoryItemRepository;
    private final InventoryReservationRepository reservationRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final NotificationServiceClient notificationServiceClient;

    // See idempotency note in SagaEventListener — same in-memory caveat applies here.
    private final Map<String, Boolean> processedEventCache = new ConcurrentHashMap<>();

    /**
     * Reserves stock for an order.
     * <p>
     * On success, writes an {@code InventoryReserved} event to the outbox in the
     * SAME transaction as the reservation. The OutboxPublisher then delivers it to
     * {@code inventory.events}. payment-service consumes that topic independently
     * and owns its own retry/circuit-breaker/DLT pipeline — inventory-service does
     * NOT call payment-service directly. This keeps the two services decoupled:
     * if payment-service is down, inventory-service is completely unaffected: the
     * event just sits in Kafka until payment-service (or its DLT reprocessor)
     * catches up. Compensation flows back asynchronously via the
     * {@code payment.events} topic ({@code PaymentFailed} -> releaseReservation).
     */
    @Transactional
    @Retryable(
            value = {OptimisticLockingFailureException.class, DataAccessException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 100, multiplier = 2.0)
    )
    public void reserve(String orderId, Map<String, Object> incomingPayload) {
        String eventKey = "reserve:" + orderId;
        if (processedEventCache.putIfAbsent(eventKey, Boolean.TRUE) != null) {
            log.info("Duplicate reserve event detected for order {}, skipping", orderId);
            return;
        }

        Optional<InventoryReservation> existingReservation =
                reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatus.RESERVED);

        if (existingReservation.isPresent()) {
            log.info("Reservation already exists for order {}, skipping", orderId);
            processedEventCache.remove(eventKey);
            return;
        }

        try {
            String productId = (String) incomingPayload.get("productId");
            Integer qty = (Integer) incomingPayload.get("quantity");

            if (productId == null || qty == null) {
                log.error("Invalid payload for order {}: missing productId or quantity", orderId);
                // ONLY send to failure topic - DO NOT send to inventory.events
                sendFailureToTopic(orderId, incomingPayload, "Invalid order payload: missing productId or quantity");
                processedEventCache.remove(eventKey);
                return;
            }

            Optional<InventoryItem> itemOpt = inventoryItemRepository.findByProductId(productId);

            // BUSINESS FAILURE - insufficient stock
            if (itemOpt.isEmpty() || itemOpt.get().getAvailableQty() < qty) {
                InventoryReservation r = InventoryReservation.builder()
                        .orderId(orderId)
                        .productId(productId)
                        .quantity(qty)
                        .status(ReservationStatus.FAILED)
                        .build();
                reservationRepository.save(r);

                String reason = itemOpt.isEmpty()
                        ? "Product not found: " + productId
                        : "Insufficient stock. Available: " + itemOpt.get().getAvailableQty() + ", Requested: " + qty;

                // ONLY send to failure topic - DO NOT send to inventory.events
                sendFailureToTopic(orderId, incomingPayload, reason);
                log.warn("Reservation FAILED for order {} (product {}): {}", orderId, productId, reason);
                processedEventCache.remove(eventKey);
                return;
            }

            // SUCCESS - Reserve inventory
            InventoryItem item = itemOpt.get();
            item.setAvailableQty(item.getAvailableQty() - qty);
            item.setReservedQty(item.getReservedQty() + qty);
            inventoryItemRepository.save(item);

            InventoryReservation r = InventoryReservation.builder()
                    .orderId(orderId)
                    .productId(productId)
                    .quantity(qty)
                    .status(ReservationStatus.RESERVED)
                    .build();
            r = reservationRepository.save(r);

            Map<String, Object> payload = new HashMap<>(incomingPayload);
            payload.put("reservationId", r.getId().toString());
            payload.put("reservedAt", Instant.now().toString());

            // SUCCESS - ONLY send to inventory.events (NOT failure topic).
            // payment-service picks this up asynchronously; inventory-service's
            // responsibility ends here.
            saveOutbox(orderId, "InventoryReserved", KafkaTopicConfig.INVENTORY_EVENTS_TOPIC, payload);
            log.info("Reservation RESERVED for order {} (product {}, qty {})", orderId, productId, qty);

        } catch (Exception e) {
            log.error("Error reserving inventory for order {}: {}", orderId, e.getMessage(), e);
            processedEventCache.remove(eventKey);
            throw e;
        }

        processedEventCache.remove(eventKey);
    }

    public void publishFailure(String orderId, Map<String, Object> payload, String reason) {
        sendFailureToTopic(orderId, payload, reason);
    }

    /**
     * Compensating action: releases a previously RESERVED reservation.
     * Triggered by {@code PaymentFailed} on {@code payment.events},
     * {@code NotificationFailed} on {@code notification.events}, or the
     * {@code inventory.events-dlt} consumer when payment-service was
     * unreachable after exhausting its own retries.
     */
    @Transactional
    @Retryable(
            value = {OptimisticLockingFailureException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 100, multiplier = 2.0)
    )
    public void releaseReservation(String orderId, String reason) {
        String eventKey = "release:" + orderId;
        if (processedEventCache.putIfAbsent(eventKey, Boolean.TRUE) != null) {
            log.info("Duplicate release event detected for order {}, skipping", orderId);
            return;
        }

        try {
            Optional<InventoryReservation> reservationOpt =
                    reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatus.RESERVED);

            if (reservationOpt.isEmpty()) {
                log.info("No active reservation found for order {}, already released or not reserved", orderId);
                processedEventCache.remove(eventKey);
                return;
            }

            InventoryReservation r = reservationOpt.get();
            Optional<InventoryItem> itemOpt = inventoryItemRepository.findByProductId(r.getProductId());

            if (itemOpt.isPresent()) {
                InventoryItem item = itemOpt.get();
                item.setAvailableQty(item.getAvailableQty() + r.getQuantity());
                item.setReservedQty(Math.max(0, item.getReservedQty() - r.getQuantity()));
                inventoryItemRepository.save(item);
            }

            r.setStatus(ReservationStatus.RELEASED);
            reservationRepository.save(r);

            Map<String, Object> payload = new HashMap<>();
            payload.put("orderId", orderId);
            payload.put("reservationId", r.getId().toString());
            payload.put("reason", reason);
            payload.put("releasedAt", Instant.now().toString());

            // SUCCESS - ONLY send to inventory.events
            saveOutbox(orderId, "InventoryReleased", KafkaTopicConfig.INVENTORY_EVENTS_TOPIC, payload);
            log.warn("Reservation RELEASED for order {} (compensating). Reason: {}", orderId, reason);

            notifyRelease(orderId, reason);

        } catch (Exception e) {
            log.error("Error releasing reservation for order {}: {}", orderId, e.getMessage(), e);
            processedEventCache.remove(eventKey);
            throw e;
        }

        processedEventCache.remove(eventKey);
    }

    @CircuitBreaker(name = NOTIFICATION_SERVICE_CB)
    private void notifyRelease(String orderId, String reason) {
        try {
            NotificationRequest notificationRequest = NotificationRequest.builder()
                    .orderId(orderId)
                    .customerId("customer-" + orderId)
                    .message("Inventory released for order " + orderId + " due to: " + reason)
                    .type("INVENTORY_RELEASED")
                    .build();

            notificationServiceClient.sendNotification(notificationRequest);
            log.info("Release notification sent for order: {}", orderId);
        } catch (Exception e) {
            log.error("Failed to send release notification for order {}: {}", orderId, e.getMessage());
        }
    }

    /**
     * ONLY sends to failure topic - NEVER to inventory.events
     */
    private void sendFailureToTopic(String orderId, Map<String, Object> incomingPayload, String reason) {
        try {
            Map<String, Object> failureEvent = new HashMap<>();
            failureEvent.put("orderId", orderId);
            failureEvent.put("service", "inventory-service");
            failureEvent.put("failureType", "BUSINESS");
            failureEvent.put("reason", reason);
            failureEvent.put("payload", incomingPayload);
            failureEvent.put("timestamp", Instant.now().toString());

            String failureJson = objectMapper.writeValueAsString(failureEvent);
            kafkaTemplate.send(KafkaTopicConfig.INVENTORY_EVENTS_FAILURE_TOPIC, orderId, failureJson);
            log.warn("[FAILURE-TOPIC] Business failure - Order {} → inventory.events-failure | reason: {}", orderId, reason);
        } catch (Exception e) {
            log.error("[FAILURE-TOPIC] Failed to send to failure topic for order {}: {}", orderId, e.getMessage());
        }
        // ⚠️ IMPORTANT: Do NOT call saveOutbox() here!
        // This method ONLY sends to the failure topic
    }

    /**
     * Saves outbox event - ONLY for SUCCESSFUL events
     * This goes to inventory.events topic
     */
    private void saveOutbox(String aggregateId, String eventType, String topic, Map<String, Object> payload) {
        try {
            String eventKey = "outbox:" + aggregateId + ":" + eventType;
            if (processedEventCache.putIfAbsent(eventKey, Boolean.TRUE) != null) {
                log.debug("Duplicate outbox entry detected for {}-{}, skipping", aggregateId, eventType);
                processedEventCache.remove(eventKey);
                return;
            }

            SagaEvent event = SagaEvent.create(aggregateId, eventType, "inventory-service", payload);
            String json = objectMapper.writeValueAsString(event);

            OutboxEvent outbox = OutboxEvent.builder()
                    .aggregateId(aggregateId)
                    .eventType(eventType)
                    .topic(topic)
                    .payload(json)
                    .partitionKey(aggregateId)
                    .build();

            outboxEventRepository.save(outbox);
            log.debug("Outbox event saved: {} for order {}", eventType, aggregateId);

            processedEventCache.remove(eventKey);
        } catch (Exception e) {
            log.error("Failed to save outbox event for order {}: {}", aggregateId, e.getMessage(), e);
            throw new RuntimeException("Failed to serialize saga event", e);
        }
    }

    public void cleanupCache() {
        processedEventCache.clear();
        log.debug("Cleaned up processed event cache");
    }
}