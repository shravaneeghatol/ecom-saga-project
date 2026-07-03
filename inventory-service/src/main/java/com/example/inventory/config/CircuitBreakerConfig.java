package com.example.inventory.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class CircuitBreakerConfig {

    public static final String KAFKA_CB = "kafkaPublisher";
    public static final String PAYMENT_SERVICE_CB = "paymentServiceCB";
    public static final String NOTIFICATION_SERVICE_CB = "notificationServiceCB";
    public static final String OUTBOX_PROCESSOR_CB = "outboxProcessorCB";

    private final CircuitBreakerRegistry registry;
    private final RetryRegistry retryRegistry;

    @PostConstruct
    public void wire() {
        // Kafka Circuit Breaker
        CircuitBreaker kafkaCb = registry.circuitBreaker(KAFKA_CB);
        kafkaCb.getEventPublisher()
                .onStateTransition(e -> log.warn(
                        "[CB:kafkaPublisher] state change: {} -> {}",
                        e.getStateTransition().getFromState(),
                        e.getStateTransition().getToState()))
                .onError(e -> log.error(
                        "[CB:kafkaPublisher] call failed {}ms: {}",
                        e.getElapsedDuration().toMillis(),
                        e.getThrowable() != null ? e.getThrowable().getMessage() : "n/a"))
                .onCallNotPermitted(e -> log.warn(
                        "[CB:kafkaPublisher] OPEN — skipping Kafka call, outbox row stays PENDING"))
                .onSuccess(e -> log.debug(
                        "[CB:kafkaPublisher] OK in {}ms",
                        e.getElapsedDuration().toMillis()));

        // Payment Service Circuit Breaker (for HTTP calls)
        CircuitBreaker paymentCb = registry.circuitBreaker(PAYMENT_SERVICE_CB);
        paymentCb.getEventPublisher()
                .onStateTransition(e -> log.warn(
                        "[CB:paymentService] state change: {} -> {}",
                        e.getStateTransition().getFromState(),
                        e.getStateTransition().getToState()))
                .onError(e -> log.error(
                        "[CB:paymentService] call failed {}ms: {}",
                        e.getElapsedDuration().toMillis(),
                        e.getThrowable() != null ? e.getThrowable().getMessage() : "n/a"))
                .onCallNotPermitted(e -> log.warn(
                        "[CB:paymentService] OPEN — payment service unavailable, skipping call"))
                .onSuccess(e -> log.debug(
                        "[CB:paymentService] OK in {}ms",
                        e.getElapsedDuration().toMillis()));

        // Notification Service Circuit Breaker (for HTTP calls)
        CircuitBreaker notificationCb = registry.circuitBreaker(NOTIFICATION_SERVICE_CB);
        notificationCb.getEventPublisher()
                .onStateTransition(e -> log.warn(
                        "[CB:notificationService] state change: {} -> {}",
                        e.getStateTransition().getFromState(),
                        e.getStateTransition().getToState()))
                .onError(e -> log.error(
                        "[CB:notificationService] call failed {}ms: {}",
                        e.getElapsedDuration().toMillis(),
                        e.getThrowable() != null ? e.getThrowable().getMessage() : "n/a"))
                .onCallNotPermitted(e -> log.warn(
                        "[CB:notificationService] OPEN — notification service unavailable, skipping call"))
                .onSuccess(e -> log.debug(
                        "[CB:notificationService] OK in {}ms",
                        e.getElapsedDuration().toMillis()));

        // Outbox Processor Circuit Breaker
        CircuitBreaker outboxCb = registry.circuitBreaker(OUTBOX_PROCESSOR_CB);
        outboxCb.getEventPublisher()
                .onStateTransition(e -> log.warn(
                        "[CB:outboxProcessor] state change: {} -> {}",
                        e.getStateTransition().getFromState(),
                        e.getStateTransition().getToState()))
                .onError(e -> log.error(
                        "[CB:outboxProcessor] call failed {}ms: {}",
                        e.getElapsedDuration().toMillis(),
                        e.getThrowable() != null ? e.getThrowable().getMessage() : "n/a"))
                .onCallNotPermitted(e -> log.warn(
                        "[CB:outboxProcessor] OPEN — outbox processing skipped, will retry later"))
                .onSuccess(e -> log.debug(
                        "[CB:outboxProcessor] OK in {}ms",
                        e.getElapsedDuration().toMillis()));

        // Setup Retry configurations
        setupRetryConfigurations();
    }

    private void setupRetryConfigurations() {
        // Retry for Kafka publishing
        Retry kafkaRetry = retryRegistry.retry("kafkaRetry");
        kafkaRetry.getEventPublisher()
                .onRetry(e -> log.warn(
                        "[Retry:kafka] Attempt {} for Kafka publish",
                        e.getNumberOfRetryAttempts()))
                .onError(e -> log.error(
                        "[Retry:kafka] Failed after {} attempts",
                        e.getNumberOfRetryAttempts()));

        // Retry for Payment Service calls
        Retry paymentRetry = retryRegistry.retry("paymentRetry");
        paymentRetry.getEventPublisher()
                .onRetry(e -> log.warn(
                        "[Retry:payment] Attempt {} for payment service call",
                        e.getNumberOfRetryAttempts()))
                .onError(e -> log.error(
                        "[Retry:payment] Failed after {} attempts",
                        e.getNumberOfRetryAttempts()));

        // Retry for Notification Service calls
        Retry notificationRetry = retryRegistry.retry("notificationRetry");
        notificationRetry.getEventPublisher()
                .onRetry(e -> log.warn(
                        "[Retry:notification] Attempt {} for notification service call",
                        e.getNumberOfRetryAttempts()))
                .onError(e -> log.error(
                        "[Retry:notification] Failed after {} attempts",
                        e.getNumberOfRetryAttempts()));
    }
}