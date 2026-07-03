package com.example.notification.service;

import com.example.notification.client.EmailServiceClient;
import com.example.notification.client.SmsServiceClient;
import com.example.notification.config.KafkaTopicConfig;
import com.example.notification.domain.*;
import com.example.notification.dto.EmailRequest;
import com.example.notification.dto.EmailResponse;
import com.example.notification.dto.SmsRequest;
import com.example.notification.dto.SmsResponse;
import com.example.notification.event.SagaEvent;
import com.example.notification.repository.NotificationRepository;
import com.example.notification.repository.OutboxEventRepository;
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

import static com.example.notification.config.CircuitBreakerConfig.EMAIL_SERVICE_CB;
import static com.example.notification.config.CircuitBreakerConfig.SMS_SERVICE_CB;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final NotificationRepository notificationRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final EmailServiceClient emailServiceClient;
    private final SmsServiceClient smsServiceClient;

    // Idempotency cache for processed events
    private final Map<String, Boolean> processedEventCache = new ConcurrentHashMap<>();

    /**
     * FINAL saga step: notify the customer.
     * Supports idempotency - duplicate events are ignored.
     */
    @Transactional
    @Retryable(
            value = {OptimisticLockingFailureException.class, DataAccessException.class},
            maxAttempts = 3,
            backoff = @Backoff(delay = 100, multiplier = 2.0)
    )
    public void sendNotification(String orderId, Map<String, Object> incomingPayload) {
        // Idempotency check - prevent duplicate processing
        String eventKey = "notification:" + orderId;
        if (processedEventCache.putIfAbsent(eventKey, Boolean.TRUE) != null) {
            log.info("Duplicate notification event detected for order {}, skipping", orderId);
            return;
        }

        try {
            // Check if notification already exists
            Optional<Notification> existingNotification =
                    notificationRepository.findByOrderIdAndStatus(orderId, NotificationStatus.SENT);

            if (existingNotification.isPresent()) {
                log.info("Notification already sent for order {}, skipping", orderId);
                processedEventCache.remove(eventKey);
                return;
            }

            String customerId = (String) incomingPayload.get("customerId");
            if (customerId == null || customerId.isEmpty()) {
                log.error("Missing customerId for order {}", orderId);
                publishFailure(orderId, incomingPayload, "Missing customerId");
                processedEventCache.remove(eventKey);
                return;
            }

            String customerEmail = (String) incomingPayload.getOrDefault(
                    "customerEmail", customerId + "@example.com");
            String customerPhone = (String) incomingPayload.getOrDefault(
                    "customerPhone", "+1234567890");

            // Build notification message
            String message = buildNotificationMessage(orderId, incomingPayload);

            // Save notification to DB
            Notification n = Notification.builder()
                    .orderId(orderId)
                    .customerId(customerId)
                    .message(message)
                    .status(NotificationStatus.SENT)
                    .build();
            n = notificationRepository.save(n);

            // Save to outbox
            Map<String, Object> payload = new HashMap<>(incomingPayload);
            payload.put("notificationId", n.getId().toString());
            payload.put("notificationMessage", message);
            payload.put("sentAt", Instant.now().toString());

            saveOutbox(orderId, "NotificationSent", KafkaTopicConfig.NOTIFICATION_EVENTS_TOPIC, payload);
            log.info("Notification SENT for order {}", orderId);

            // Send email notification (with circuit breaker)
            boolean emailSent = sendEmailNotification(orderId, customerEmail, message);

            // Send SMS notification (with circuit breaker)
            boolean smsSent = sendSmsNotification(orderId, customerPhone, message);

            // If both email and SMS failed, mark as partial failure but don't fail the saga
            if (!emailSent && !smsSent) {
                log.warn("Both email and SMS failed for order {} - notification marked as sent but channels failed", orderId);
                // Update notification status to reflect partial failure
                n.setStatus(NotificationStatus.PARTIAL);
                notificationRepository.save(n);
            } else if (!emailSent) {
                log.warn("Email failed but SMS succeeded for order {}", orderId);
                n.setStatus(NotificationStatus.PARTIAL);
                notificationRepository.save(n);
            } else if (!smsSent) {
                log.warn("SMS failed but email succeeded for order {}", orderId);
                n.setStatus(NotificationStatus.PARTIAL);
                notificationRepository.save(n);
            }

        } catch (Exception e) {
            log.error("Error sending notification for order {}: {}", orderId, e.getMessage(), e);
            processedEventCache.remove(eventKey);
            throw e;
        }

        processedEventCache.remove(eventKey);
    }

    /**
     * Build notification message based on payload
     */
    private String buildNotificationMessage(String orderId, Map<String, Object> payload) {
        StringBuilder message = new StringBuilder();
        message.append("Your order ").append(orderId).append(" has been placed successfully!\n");
        message.append("Details:\n");

        // Add order details if available
        if (payload.containsKey("productId")) {
            message.append("Product: ").append(payload.get("productId")).append("\n");
        }
        if (payload.containsKey("quantity")) {
            message.append("Quantity: ").append(payload.get("quantity")).append("\n");
        }
        if (payload.containsKey("amount") || payload.containsKey("totalAmount")) {
            Double amount = (Double) payload.getOrDefault("totalAmount",
                    payload.getOrDefault("amount", 0.0));
            message.append("Amount: $").append(String.format("%.2f", amount)).append("\n");
        }
        message.append("\nThank you for your business!");

        return message.toString();
    }

    /**
     * Send email notification with Circuit Breaker protection
     */
    @CircuitBreaker(name = EMAIL_SERVICE_CB)
    @Retryable(
            value = {Exception.class},
            maxAttempts = 2,
            backoff = @Backoff(delay = 1000, multiplier = 2.0)
    )
    private boolean sendEmailNotification(String orderId, String customerEmail, String message) {
        try {
            EmailRequest request = EmailRequest.builder()
                    .to(customerEmail)
                    .subject("Order Confirmation - " + orderId)
                    .body(message)
                    .from("noreply@ecommerce.com")
                    .build();

            EmailResponse response = emailServiceClient.sendEmail(request);
            boolean success = response != null && response.isSuccess();

            if (success) {
                log.info("Email notification sent successfully for order: {}", orderId);
            } else {
                log.warn("Email notification returned failure for order {}: {}",
                        orderId, response != null ? response.getMessage() : "unknown");
            }

            return success;
        } catch (Exception e) {
            log.error("Failed to send email notification for order {}: {}", orderId, e.getMessage());
            return false;
        }
    }

    /**
     * Send SMS notification with Circuit Breaker protection
     */
    @CircuitBreaker(name = SMS_SERVICE_CB)
    @Retryable(
            value = {Exception.class},
            maxAttempts = 2,
            backoff = @Backoff(delay = 1000, multiplier = 2.0)
    )
    private boolean sendSmsNotification(String orderId, String customerPhone, String message) {
        try {
            // Truncate message for SMS if too long
            String smsMessage = message.length() > 160 ? message.substring(0, 157) + "..." : message;

            SmsRequest request = SmsRequest.builder()
                    .phoneNumber(customerPhone)
                    .message("Order " + orderId + " confirmed. " + smsMessage)
                    .sender("ECOMMERCE")
                    .build();

            SmsResponse response = smsServiceClient.sendSms(request);
            boolean success = response != null && response.isSuccess();

            if (success) {
                log.info("SMS notification sent successfully for order: {}", orderId);
            } else {
                log.warn("SMS notification returned failure for order {}: {}",
                        orderId, response != null ? response.getMessage() : "unknown");
            }

            return success;
        } catch (Exception e) {
            log.error("Failed to send SMS notification for order {}: {}", orderId, e.getMessage());
            return false;
        }
    }

    /**
     * Publish failure with comprehensive error details
     */
    @Transactional
    public void publishFailure(String orderId, Map<String, Object> incomingPayload, String reason) {
        // Idempotency check
        String eventKey = "failure:" + orderId;
        if (processedEventCache.putIfAbsent(eventKey, Boolean.TRUE) != null) {
            log.info("Duplicate failure publish for order {}, skipping", orderId);
            return;
        }

        try {
            // Save failed notification
            Notification n = Notification.builder()
                    .orderId(orderId)
                    .customerId((String) incomingPayload.getOrDefault("customerId", "unknown"))
                    .message("Failed to notify customer for order " + orderId)
                    .status(NotificationStatus.FAILED)
                    .build();
            notificationRepository.save(n);

            Map<String, Object> payload = new HashMap<>(incomingPayload);
            payload.put("reason", reason);
            payload.put("failedAt", Instant.now().toString());
            payload.put("notificationId", n.getId().toString());

            // 1. Saga compensation - triggers payment refund and inventory release
            saveOutbox(orderId, "NotificationFailed", KafkaTopicConfig.NOTIFICATION_EVENTS_TOPIC, payload);

            // 2. Send to failure topic
            sendToFailureTopic(orderId, incomingPayload, reason);

            // 3. Try to send failure notification via alternative channel
            sendFailureNotification(orderId, reason);

        } catch (Exception e) {
            log.error("Error publishing failure for order {}: {}", orderId, e.getMessage(), e);
        } finally {
            processedEventCache.remove(eventKey);
        }
    }

    @CircuitBreaker(name = EMAIL_SERVICE_CB)
    private void sendFailureNotification(String orderId, String reason) {
        try {
            // Try to notify via email about the failure
            String customerId = "customer-" + orderId; // Placeholder
            String customerEmail = customerId + "@example.com";

            EmailRequest request = EmailRequest.builder()
                    .to(customerEmail)
                    .subject("Order Notification Failed - " + orderId)
                    .body("We apologize, but we were unable to send the order confirmation for order " +
                            orderId + ". Reason: " + reason +
                            "\n\nPlease contact support for assistance.")
                    .from("noreply@ecommerce.com")
                    .build();

            emailServiceClient.sendEmail(request);
            log.info("Failure notification sent for order: {}", orderId);
        } catch (Exception e) {
            log.error("Failed to send failure notification for order {}: {}", orderId, e.getMessage());
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

            SagaEvent event = SagaEvent.create(aggregateId, eventType, "notification-service", payload);
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
            failureEvent.put("service", "notification-service");
            failureEvent.put("failureType", "BUSINESS");
            failureEvent.put("reason", reason);
            failureEvent.put("payload", incomingPayload);
            failureEvent.put("timestamp", Instant.now().toString());

            String failureJson = objectMapper.writeValueAsString(failureEvent);
            kafkaTemplate.send(KafkaTopicConfig.NOTIFICATION_EVENTS_FAILURE_TOPIC, orderId, failureJson);
            log.warn("[FAILURE-TOPIC] Order {} → notification.events-failure | reason: {}", orderId, reason);
        } catch (Exception e) {
            log.error("[FAILURE-TOPIC] Failed to send to notification.events-failure for order {}: {}",
                    orderId, e.getMessage());
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