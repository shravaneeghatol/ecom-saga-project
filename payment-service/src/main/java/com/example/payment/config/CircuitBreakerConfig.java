package com.example.payment.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

/**
 * Enhanced Circuit Breaker Configuration with multiple breakers for:
 * 1. Kafka Publisher - protects outbox publishing to Kafka
 * 2. Notification Service - protects HTTP calls to notification service
 * 3. Order Service - protects HTTP calls to order service for compensation
 * 4. Outbox Processor - protects outbox processing scheduler
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class CircuitBreakerConfig {

    public static final String KAFKA_PUBLISHER_BREAKER = "kafkaPublisher";
    public static final String NOTIFICATION_SERVICE_CB = "notificationServiceCB";
    public static final String ORDER_SERVICE_CB = "orderServiceCB";
    public static final String OUTBOX_PROCESSOR_CB = "outboxProcessorCB";

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;

    @PostConstruct
    public void registerEventListeners() {
        // 1. Kafka Publisher Circuit Breaker
        CircuitBreaker kafkaBreaker = circuitBreakerRegistry.circuitBreaker(KAFKA_PUBLISHER_BREAKER);
        kafkaBreaker.getEventPublisher()
                .onStateTransition(event -> log.warn(
                        "[CB:kafkaPublisher] state transition: {} -> {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()))
                .onError(event -> log.error(
                        "[CB:kafkaPublisher] call failed after {} ms: {}",
                        event.getElapsedDuration().toMillis(),
                        event.getThrowable() == null ? "n/a" : event.getThrowable().toString()))
                .onCallNotPermitted(event -> log.warn(
                        "[CB:kafkaPublisher] call rejected - breaker is OPEN, failing fast"))
                .onSuccess(event -> log.debug(
                        "[CB:kafkaPublisher] call succeeded in {} ms",
                        event.getElapsedDuration().toMillis()));

        // 2. Notification Service Circuit Breaker
        CircuitBreaker notificationBreaker = circuitBreakerRegistry.circuitBreaker(NOTIFICATION_SERVICE_CB);
        notificationBreaker.getEventPublisher()
                .onStateTransition(event -> log.warn(
                        "[CB:notificationService] state transition: {} -> {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()))
                .onError(event -> log.error(
                        "[CB:notificationService] call failed after {} ms: {}",
                        event.getElapsedDuration().toMillis(),
                        event.getThrowable() == null ? "n/a" : event.getThrowable().toString()))
                .onCallNotPermitted(event -> log.warn(
                        "[CB:notificationService] call rejected - notification service unavailable"))
                .onSuccess(event -> log.debug(
                        "[CB:notificationService] call succeeded in {} ms",
                        event.getElapsedDuration().toMillis()));

        // 3. Order Service Circuit Breaker (for compensation calls)
        CircuitBreaker orderBreaker = circuitBreakerRegistry.circuitBreaker(ORDER_SERVICE_CB);
        orderBreaker.getEventPublisher()
                .onStateTransition(event -> log.warn(
                        "[CB:orderService] state transition: {} -> {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()))
                .onError(event -> log.error(
                        "[CB:orderService] call failed after {} ms: {}",
                        event.getElapsedDuration().toMillis(),
                        event.getThrowable() == null ? "n/a" : event.getThrowable().toString()))
                .onCallNotPermitted(event -> log.warn(
                        "[CB:orderService] call rejected - order service unavailable"))
                .onSuccess(event -> log.debug(
                        "[CB:orderService] call succeeded in {} ms",
                        event.getElapsedDuration().toMillis()));

        // 4. Outbox Processor Circuit Breaker
        CircuitBreaker outboxBreaker = circuitBreakerRegistry.circuitBreaker(OUTBOX_PROCESSOR_CB);
        outboxBreaker.getEventPublisher()
                .onStateTransition(event -> log.warn(
                        "[CB:outboxProcessor] state transition: {} -> {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()))
                .onError(event -> log.error(
                        "[CB:outboxProcessor] call failed after {} ms: {}",
                        event.getElapsedDuration().toMillis(),
                        event.getThrowable() == null ? "n/a" : event.getThrowable().toString()))
                .onCallNotPermitted(event -> log.warn(
                        "[CB:outboxProcessor] call rejected - outbox processing skipped"))
                .onSuccess(event -> log.debug(
                        "[CB:outboxProcessor] call succeeded in {} ms",
                        event.getElapsedDuration().toMillis()));

        // Setup Retry configurations
        setupRetryConfigurations();
    }

    private void setupRetryConfigurations() {
        // Retry for Kafka publishing
        Retry kafkaRetry = retryRegistry.retry("kafkaRetry");
        kafkaRetry.getEventPublisher()
                .onRetry(event -> log.warn(
                        "[Retry:kafka] Attempt {} for Kafka publish",
                        event.getNumberOfRetryAttempts()))
                .onError(event -> log.error(
                        "[Retry:kafka] Failed after {} attempts",
                        event.getNumberOfRetryAttempts()));

        // Retry for Notification Service calls
        Retry notificationRetry = retryRegistry.retry("notificationRetry");
        notificationRetry.getEventPublisher()
                .onRetry(event -> log.warn(
                        "[Retry:notification] Attempt {} for notification service call",
                        event.getNumberOfRetryAttempts()))
                .onError(event -> log.error(
                        "[Retry:notification] Failed after {} attempts",
                        event.getNumberOfRetryAttempts()));

        // Retry for Order Service calls
        Retry orderRetry = retryRegistry.retry("orderRetry");
        orderRetry.getEventPublisher()
                .onRetry(event -> log.warn(
                        "[Retry:order] Attempt {} for order service call",
                        event.getNumberOfRetryAttempts()))
                .onError(event -> log.error(
                        "[Retry:order] Failed after {} attempts",
                        event.getNumberOfRetryAttempts()));
    }
}