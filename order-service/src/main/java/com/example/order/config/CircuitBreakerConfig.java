package com.example.order.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

/**
 * Registers event listeners for ALL circuit breakers used in order-service.
 *
 * ┌──────────────────────┬────────────────────────────────────────────────────┐
 * │ Breaker name         │ What it protects                                   │
 * ├──────────────────────┼────────────────────────────────────────────────────┤
 * │ kafkaPublisher       │ OutboxPublisher → KafkaEventSender.sendToKafka()   │
 * │                      │ (blocking .get(10s) Kafka send in the outbox poller)│
 * ├──────────────────────┼────────────────────────────────────────────────────┤
 * │ inventoryService     │ HTTP calls to inventory-service                    │
 * │ paymentService       │ HTTP calls to payment-service                      │
 * │ notificationService  │ HTTP calls to notification-service                 │
 * └──────────────────────┴────────────────────────────────────────────────────┘
 *
 * All four breakers are configured in application.yml under
 * resilience4j.circuitbreaker.instances.*
 *
 * State machine reminder:
 *  CLOSED    → normal; calls pass through.
 *  OPEN      → failure threshold exceeded; calls rejected immediately (fast-fail).
 *  HALF_OPEN → trial period after waitDurationInOpenState; limited calls let through.
 *              If they succeed → back to CLOSED.  If they fail → back to OPEN.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class CircuitBreakerConfig {

    // ── Breaker names (must match application.yml keys exactly) ───────────────
    public static final String KAFKA_PUBLISHER_BREAKER    = "kafkaPublisher";
    public static final String INVENTORY_SERVICE_BREAKER  = "inventoryService";
    public static final String PAYMENT_SERVICE_BREAKER    = "paymentService";
    public static final String NOTIFICATION_SERVICE_BREAKER = "notificationService";

    private final CircuitBreakerRegistry circuitBreakerRegistry;

    @PostConstruct
    public void registerEventListeners() {
        attachListeners(KAFKA_PUBLISHER_BREAKER);
        attachListeners(INVENTORY_SERVICE_BREAKER);
        attachListeners(PAYMENT_SERVICE_BREAKER);
        attachListeners(NOTIFICATION_SERVICE_BREAKER);
    }

    /**
     * Attaches state-transition, error, call-not-permitted, and success listeners
     * to the named breaker. All four breakers get the same logging behaviour so
     * you have a uniform audit trail in the logs.
     */
    private void attachListeners(String breakerName) {
        CircuitBreaker breaker = circuitBreakerRegistry.circuitBreaker(breakerName);

        breaker.getEventPublisher()

                // ── State change (CLOSED → OPEN, OPEN → HALF_OPEN, etc.) ──────
                .onStateTransition(event -> log.warn(
                        "[CIRCUIT-BREAKER:{}] ⚡ STATE CHANGE: {} → {}",
                        breakerName,
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()))

                // ── Individual call failure ────────────────────────────────────
                .onError(event -> log.error(
                        "[CIRCUIT-BREAKER:{}] ❌ call FAILED after {} ms | error: {}",
                        breakerName,
                        event.getElapsedDuration().toMillis(),
                        event.getThrowable() == null ? "n/a" : event.getThrowable().toString()))

                // ── Call rejected because breaker is OPEN ──────────────────────
                .onCallNotPermitted(event -> log.warn(
                        "[CIRCUIT-BREAKER:{}] 🚫 call REJECTED — breaker is OPEN, failing fast",
                        breakerName))

                // ── Successful call ────────────────────────────────────────────
                .onSuccess(event -> log.debug(
                        "[CIRCUIT-BREAKER:{}] ✅ call SUCCEEDED in {} ms",
                        breakerName,
                        event.getElapsedDuration().toMillis()))

                // ── Slow call (exceeds slow-call-duration-threshold) ───────────
                .onSlowCallRateExceeded(event -> log.warn(
                        "[CIRCUIT-BREAKER:{}] 🐢 SLOW call rate exceeded threshold: {} %",
                        breakerName,
                        event.getSlowCallRate()))

                // ── Failure rate exceeded (just before OPEN transition) ────────
                .onFailureRateExceeded(event -> log.warn(
                        "[CIRCUIT-BREAKER:{}] 🔥 FAILURE rate exceeded threshold: {} %",
                        breakerName,
                        event.getFailureRate()));
    }
}