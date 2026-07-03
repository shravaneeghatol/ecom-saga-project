package com.example.payment.service;

import com.example.payment.client.NotificationServiceClient;
import com.example.payment.client.OrderServiceClient;
import com.example.payment.config.KafkaTopicConfig;
import com.example.payment.domain.*;
import com.example.payment.dto.NotificationRequest;
import com.example.payment.dto.NotificationResponse;
import com.example.payment.dto.OrderCompensationResponse;
import com.example.payment.event.SagaEvent;
import com.example.payment.repository.OutboxEventRepository;
import com.example.payment.repository.PaymentRepository;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static com.example.payment.config.CircuitBreakerConfig.NOTIFICATION_SERVICE_CB;
import static com.example.payment.config.CircuitBreakerConfig.ORDER_SERVICE_CB;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final NotificationServiceClient notificationServiceClient;
    private final OrderServiceClient orderServiceClient;

    // Idempotency cache for processed events
    private final Map<String, Boolean> processedEventCache = new ConcurrentHashMap<>();

    /**
     * PRIMARY saga step: charge the customer.
     * Supports idempotency - duplicate events are ignored.
     */
    @Transactional
    @Retryable(
            value = {OptimisticLockingFailureException.class, DataAccessException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 100, multiplier = 2.0)
    )
    public void processPayment(String orderId, Map<String, Object> incomingPayload, boolean simulateFailure) {
        // Idempotency check - prevent duplicate processing
        String eventKey = "payment:" + orderId;
        if (processedEventCache.putIfAbsent(eventKey, Boolean.TRUE) != null) {
            log.info("Duplicate payment event detected for order {}, skipping", orderId);
            return;
        }

        try {
            // Check if payment already exists
            Optional<Payment> existingPayment = paymentRepository.findByOrderIdAndStatus(
                    orderId, PaymentStatus.COMPLETED);

            if (existingPayment.isPresent()) {
                log.info("Payment already completed for order {}, skipping", orderId);
                processedEventCache.remove(eventKey);
                return;
            }

            // Check for failed payment
            Optional<Payment> failedPayment = paymentRepository.findByOrderIdAndStatus(
                    orderId, PaymentStatus.FAILED);

            if (failedPayment.isPresent()) {
                log.info("Payment already failed for order {}, skipping", orderId);
                processedEventCache.remove(eventKey);
                return;
            }

            Double amount = incomingPayload.get("amount") == null ? 0.0 :
                    ((Number) incomingPayload.get("amount")).doubleValue();

            // Validate amount
            if (amount <= 0) {
                log.error("Invalid payment amount for order {}: {}", orderId, amount);
                publishFailure(orderId, incomingPayload, "Invalid payment amount: " + amount);
                processedEventCache.remove(eventKey);
                return;
            }

            // Business rule: Validate customer exists
            String customerId = (String) incomingPayload.get("customerId");
            if (customerId == null || customerId.isEmpty()) {
                log.error("Missing customerId for order {}", orderId);
                publishFailure(orderId, incomingPayload, "Missing customerId");
                processedEventCache.remove(eventKey);
                return;
            }

            // Determine if payment should fail
            boolean shouldFail = simulateFailure || isPaymentDeclinedByBusinessRule(incomingPayload);

            Payment payment = Payment.builder()
                    .orderId(orderId)
                    .amount(amount)
                    .status(shouldFail ? PaymentStatus.FAILED : PaymentStatus.COMPLETED)
                    .build();
            payment = paymentRepository.save(payment);

            Map<String, Object> payload = new HashMap<>(incomingPayload);
            payload.put("paymentId", payment.getId().toString());
            payload.put("paymentAmount", amount);
            payload.put("paymentStatus", payment.getStatus().toString());
            payload.put("processedAt", Instant.now().toString());

            if (shouldFail) {
                String reason = "Payment declined - business rule violation";
                payload.put("reason", reason);
                saveOutbox(orderId, "PaymentFailed", KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, payload);
                log.warn("Payment FAILED for order {}: {}", orderId, reason);

                // Send to failure topic
                sendToFailureTopic(orderId, incomingPayload, reason);

                // Try to notify order service about failure (with circuit breaker)
                notifyOrderService(orderId, reason);
            } else {
                saveOutbox(orderId, "PaymentCompleted", KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, payload);
                log.info("Payment COMPLETED for order {} (amount {})", orderId, amount);

                // Try to send notification (with circuit breaker)
                sendPaymentNotification(orderId, payment);
            }

        } catch (Exception e) {
            log.error("Error processing payment for order {}: {}", orderId, e.getMessage(), e);
            processedEventCache.remove(eventKey);
            throw e;
        }

        processedEventCache.remove(eventKey);
    }

    /**
     * Business rule validation for payment
     */
    private boolean isPaymentDeclinedByBusinessRule(Map<String, Object> payload) {
        // Example business rules:
        // 1. Amount > 10000 - might trigger fraud check
        Double amount = (Double) payload.getOrDefault("amount", 0.0);
        if (amount > 10000.0) {
            log.info("Payment declined: amount {} exceeds limit", amount);
            return true;
        }

        // 2. Customer blacklist check (simplified)
        String customerId = (String) payload.get("customerId");
        if ("BLACKLISTED".equals(customerId)) {
            log.info("Payment declined: customer {} is blacklisted", customerId);
            return true;
        }

        // 3. Product restrictions
        String productId = (String) payload.get("productId");
        if ("RESTRICTED-PROD".equals(productId)) {
            log.info("Payment declined: product {} is restricted", productId);
            return true;
        }

        return false;
    }

    /**
     * Send notification about successful payment with Circuit Breaker protection
     */
    @CircuitBreaker(name = NOTIFICATION_SERVICE_CB)
    @Retryable(
            value = {Exception.class},
            maxAttempts = 2,
            backoff = @Backoff(delay = 1000, multiplier = 2.0)
    )
    private void sendPaymentNotification(String orderId, Payment payment) {
        try {
            NotificationRequest request = NotificationRequest.builder()
                    .orderId(orderId)
                    .customerId("customer-" + orderId) // In production, get from order
                    .message("Payment of $" + payment.getAmount() + " processed successfully for order " + orderId)
                    .type("PAYMENT_SUCCESS")
                    .build();

            NotificationResponse response = notificationServiceClient.sendNotification(request);
            if (response != null && response.isSuccess()) {
                log.info("Payment notification sent successfully for order: {}", orderId);
            } else {
                log.warn("Payment notification failed for order {}: {}", orderId,
                        response != null ? response.getMessage() : "unknown error");
            }
        } catch (Exception e) {
            log.error("Failed to send payment notification for order {}: {}", orderId, e.getMessage());
            // Non-critical - don't throw, let outbox handle retry
        }
    }

    /**
     * Notify order service about failure with Circuit Breaker protection
     */
    @CircuitBreaker(name = ORDER_SERVICE_CB)
    @Retryable(
            value = {Exception.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 1000, multiplier = 2.0, maxDelay = 5000)
    )
    private void notifyOrderService(String orderId, String reason) {
        try {
            OrderCompensationResponse response = orderServiceClient.compensateOrder(orderId, reason);
            if (response != null && response.isSuccess()) {
                log.info("Order compensation triggered successfully for order: {}", orderId);
            } else {
                log.warn("Order compensation may have failed for order {}: {}", orderId,
                        response != null ? response.getMessage() : "unknown error");
            }
        } catch (Exception e) {
            log.error("Failed to compensate order {}: {}", orderId, e.getMessage());
            // Will be retried via outbox or manual intervention
            throw e; // Allow retry mechanism to handle
        }
    }

    /**
     * Compensation: a later step (notification) failed - refund this payment.
     * Idempotent - safe to call multiple times.
     */
    @Transactional
    @Retryable(
            value = {OptimisticLockingFailureException.class, DataAccessException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 100, multiplier = 2.0)
    )
    public void refund(String orderId, String reason) {
        // Idempotency check
        String eventKey = "refund:" + orderId;
        if (processedEventCache.putIfAbsent(eventKey, Boolean.TRUE) != null) {
            log.info("Duplicate refund event detected for order {}, skipping", orderId);
            return;
        }

        try {
            Optional<Payment> paymentOpt = paymentRepository.findByOrderIdAndStatus(
                    orderId, PaymentStatus.COMPLETED);

            if (paymentOpt.isEmpty()) {
                log.info("No completed payment found for order {}, already refunded or not paid", orderId);
                processedEventCache.remove(eventKey);
                return;
            }

            Payment payment = paymentOpt.get();

            // Check if already refunded
            if (payment.getStatus() == PaymentStatus.REFUNDED) {
                log.info("Payment already refunded for order {}, skipping", orderId);
                processedEventCache.remove(eventKey);
                return;
            }

            payment.setStatus(PaymentStatus.REFUNDED);
            paymentRepository.save(payment);

            Map<String, Object> payload = new HashMap<>();
            payload.put("orderId", orderId);
            payload.put("paymentId", payment.getId().toString());
            payload.put("amount", payment.getAmount());
            payload.put("reason", reason);
            payload.put("refundedAt", Instant.now().toString());

            saveOutbox(orderId, "PaymentRefunded", KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, payload);
            log.warn("Payment REFUNDED for order {} (compensating). Reason: {}", orderId, reason);

            // Try to send refund notification
            sendRefundNotification(orderId, reason);

        } catch (Exception e) {
            log.error("Error processing refund for order {}: {}", orderId, e.getMessage(), e);
            processedEventCache.remove(eventKey);
            throw e;
        }

        processedEventCache.remove(eventKey);
    }

    @CircuitBreaker(name = NOTIFICATION_SERVICE_CB)
    private void sendRefundNotification(String orderId, String reason) {
        try {
            NotificationRequest request = NotificationRequest.builder()
                    .orderId(orderId)
                    .customerId("customer-" + orderId)
                    .message("Payment refunded for order " + orderId + " due to: " + reason)
                    .type("PAYMENT_REFUND")
                    .build();

            notificationServiceClient.sendNotification(request);
            log.info("Refund notification sent for order: {}", orderId);
        } catch (Exception e) {
            log.error("Failed to send refund notification for order {}: {}", orderId, e.getMessage());
            // Don't throw - notification failure shouldn't block refund
        }
    }

    /**
     * Publish failure with comprehensive error details
     */
    @Transactional
    public void publishFailure(String orderId, Map<String, Object> incomingPayload, String reason) {
        Map<String, Object> payload = new HashMap<>(incomingPayload);
        payload.put("reason", reason);
        payload.put("failedAt", Instant.now().toString());

        // 1. Save failed payment if not already saved
        try {
            Optional<Payment> existingPayment = paymentRepository.findByOrderIdAndStatus(
                    orderId, PaymentStatus.FAILED);

            if (existingPayment.isEmpty()) {
                Payment payment = Payment.builder()
                        .orderId(orderId)
                        .amount((Double) incomingPayload.getOrDefault("amount", 0.0))
                        .status(PaymentStatus.FAILED)
                        .build();
                paymentRepository.save(payment);
                payload.put("paymentId", payment.getId().toString());
            }
        } catch (Exception e) {
            log.error("Failed to save failed payment record for order {}: {}", orderId, e.getMessage());
        }

        // 2. Saga compensation - triggers inventory release
        saveOutbox(orderId, "PaymentFailed", KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, payload);

        // 3. Send to failure topic for visibility
        sendToFailureTopic(orderId, incomingPayload, reason);

        // 4. Try to compensate order (with circuit breaker)
        try {
            notifyOrderService(orderId, reason);
        } catch (Exception e) {
            log.error("Order compensation failed for order {}: {}", orderId, e.getMessage());
            // Will be retried via outbox
        }
    }

    /**
     * Save outbox event with retry handling and deduplication
     */
    private void saveOutbox(String aggregateId, String eventType, String topic, Map<String, Object> payload) {
        try {
            // Deduplication check
            String eventKey = "outbox:" + aggregateId + ":" + eventType;
            if (processedEventCache.putIfAbsent(eventKey, Boolean.TRUE) != null) {
                log.debug("Duplicate outbox entry detected for {}-{}, skipping", aggregateId, eventType);
                processedEventCache.remove(eventKey);
                return;
            }

            SagaEvent event = SagaEvent.create(aggregateId, eventType, "payment-service", payload);
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

    /**
     * Send to failure topic with comprehensive error details
     */
    private void sendToFailureTopic(String orderId, Map<String, Object> incomingPayload, String reason) {
        try {
            Map<String, Object> failureEvent = new HashMap<>();
            failureEvent.put("orderId", orderId);
            failureEvent.put("service", "payment-service");
            failureEvent.put("failureType", "BUSINESS");
            failureEvent.put("reason", reason);
            failureEvent.put("payload", incomingPayload);
            failureEvent.put("timestamp", Instant.now().toString());

            String failureJson = objectMapper.writeValueAsString(failureEvent);
            kafkaTemplate.send(KafkaTopicConfig.PAYMENT_EVENTS_FAILURE_TOPIC, orderId, failureJson);
            log.warn("[FAILURE-TOPIC] Order {} → payment.events-failure | reason: {}", orderId, reason);
        } catch (Exception e) {
            log.error("[FAILURE-TOPIC] Failed to send to payment.events-failure for order {}: {}", orderId, e.getMessage());
        }
    }

    /**
     * Clean up processed events cache periodically
     */
    public void cleanupCache() {
        processedEventCache.clear();
        log.debug("Cleaned up processed event cache");
    }
}