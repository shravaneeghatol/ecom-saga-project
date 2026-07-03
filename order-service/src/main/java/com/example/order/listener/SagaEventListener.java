package com.example.order.listener;

import com.example.order.event.SagaEvent;
import com.example.order.service.OrderService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

/**
 * Listens to Kafka topics from other services and drives Order compensation or completion.
 *
 * ── Why no @CircuitBreaker on these listener methods? ───────────────────────
 *
 * The downstream calls inside these listeners are:
 *   orderService.cancelOrder(...)  → writes to local H2 DB (same JVM, no network)
 *   orderService.markCompleted(...) → same
 *
 * Circuit breakers are for protecting calls to REMOTE, potentially-unavailable
 * I/O boundaries (Kafka broker, HTTP endpoints).  Writing to a local in-process
 * H2 database does not need one.
 *
 * If you swap H2 for a remote PostgreSQL, you WOULD add a "database" circuit
 * breaker around those calls.  For now, the two I/O boundaries that ARE protected:
 *
 *   1. Kafka publish  → KafkaEventSender (@CircuitBreaker name="kafkaPublisher")
 *   2. HTTP calls     → ServiceHealthClient (@CircuitBreaker per downstream service)
 *
 * ── Retry / DLT strategy ────────────────────────────────────────────────────
 *
 * notification.events uses @RetryableTopic (4 attempts, exponential back-off).
 * inventory.events and payment.events use plain @KafkaListener (compensation
 * events are idempotent and simple enough not to need retry topics).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SagaEventListener {

    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    // ── Compensation: inventory.events ─────────────────────────────────────────

    @KafkaListener(topics = "inventory.events", groupId = "order-service-compensation-group")
    public void onInventoryEvent(SagaEvent event) {
        log.debug("[LISTENER] inventory.events → type={} orderId={}", event.getEventType(), event.getSagaId());

        if ("InventoryReservationFailed".equals(event.getEventType())) {
            String reason = String.valueOf(
                    event.getPayload().getOrDefault("reason", "inventory reservation failed"));
            log.warn("[LISTENER] InventoryReservationFailed for order {} — triggering compensation", event.getSagaId());
            orderService.cancelOrder(event.getSagaId(), reason);
        }
        // InventoryReserved / InventoryReleased → no action needed in order-service
    }

    // ── Compensation: payment.events ──────────────────────────────────────────

    @KafkaListener(topics = "payment.events", groupId = "order-service-compensation-group")
    public void onPaymentEvent(SagaEvent event) {
        log.debug("[LISTENER] payment.events → type={} orderId={}", event.getEventType(), event.getSagaId());

        if ("PaymentFailed".equals(event.getEventType())) {
            String reason = String.valueOf(
                    event.getPayload().getOrDefault("reason", "payment failed"));
            log.warn("[LISTENER] PaymentFailed for order {} — triggering compensation", event.getSagaId());
            orderService.cancelOrder(event.getSagaId(), reason);
        }
    }

    // ── PRIMARY: notification.events (final saga step) ────────────────────────

    /**
     * 4 attempts total (1 initial + 3 retries) with exponential back-off.
     * After all attempts are exhausted the message goes to notification.events-dlt
     * where handleNotificationDlt() triggers order cancellation.
     *
     * Created topics: notification.events-retry-0, -retry-1, -retry-2, -dlt
     */
    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 15000),
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = { RuntimeException.class }
    )
    @KafkaListener(topics = "notification.events", groupId = "order-service-group")
    public void onNotificationEvent(SagaEvent event) {
        log.debug("[LISTENER] notification.events → type={} orderId={}", event.getEventType(), event.getSagaId());

        if ("NotificationSent".equals(event.getEventType())) {
            orderService.markCompleted(event.getSagaId());

        } else if ("NotificationFailed".equals(event.getEventType())) {
            String reason = String.valueOf(
                    event.getPayload().getOrDefault("reason", "notification failed"));
            log.warn("[LISTENER] NotificationFailed for order {} — triggering compensation", event.getSagaId());
            orderService.cancelOrder(event.getSagaId(), reason);
        }
    }

    /**
     * DLT handler — invoked after all retry attempts for notification.events are exhausted.
     *
     * Receives the raw JSON string (not a deserialized SagaEvent) because DLT messages
     * can arrive with different headers that confuse the JsonDeserializer.
     * We manually deserialize here to be safe.
     */
    @DltHandler
    public void handleNotificationDlt(String message) {
        try {
            log.error("[DLT] notification.events DLT received. Raw: {}", message);
            SagaEvent event = objectMapper.readValue(message, SagaEvent.class);
            log.error("[DLT] Exhausted all retries for order {} — cancelling order", event.getSagaId());
            orderService.cancelOrder(event.getSagaId(),
                    "Notification step failed after max retries (DLT)");

        } catch (Exception e) {
            // Don't re-throw: we don't want an infinite DLT → retry → DLT loop.
            // The raw message remains in the DLT topic for manual inspection.
            log.error("[DLT] Failed to deserialize DLT message: {}. Raw: {}", e.getMessage(), message);
        }
    }
}