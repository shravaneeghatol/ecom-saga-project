package com.example.notification.config;

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
 * 2. Email Service - protects email sending (if using external email provider)
 * 3. SMS Service - protects SMS sending (if using external SMS provider)
 * 4. Outbox Processor - protects outbox processing scheduler
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class CircuitBreakerConfig {

    public static final String KAFKA_PUBLISHER_BREAKER = "kafkaPublisher";
    public static final String EMAIL_SERVICE_CB = "emailServiceCB";
    public static final String SMS_SERVICE_CB = "smsServiceCB";
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

        // 2. Email Service Circuit Breaker (for external email provider)
        CircuitBreaker emailBreaker = circuitBreakerRegistry.circuitBreaker(EMAIL_SERVICE_CB);
        emailBreaker.getEventPublisher()
                .onStateTransition(event -> log.warn(
                        "[CB:emailService] state transition: {} -> {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()))
                .onError(event -> log.error(
                        "[CB:emailService] call failed after {} ms: {}",
                        event.getElapsedDuration().toMillis(),
                        event.getThrowable() == null ? "n/a" : event.getThrowable().toString()))
                .onCallNotPermitted(event -> log.warn(
                        "[CB:emailService] call rejected - email service unavailable"))
                .onSuccess(event -> log.debug(
                        "[CB:emailService] call succeeded in {} ms",
                        event.getElapsedDuration().toMillis()));

        // 3. SMS Service Circuit Breaker (for external SMS provider)
        CircuitBreaker smsBreaker = circuitBreakerRegistry.circuitBreaker(SMS_SERVICE_CB);
        smsBreaker.getEventPublisher()
                .onStateTransition(event -> log.warn(
                        "[CB:smsService] state transition: {} -> {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()))
                .onError(event -> log.error(
                        "[CB:smsService] call failed after {} ms: {}",
                        event.getElapsedDuration().toMillis(),
                        event.getThrowable() == null ? "n/a" : event.getThrowable().toString()))
                .onCallNotPermitted(event -> log.warn(
                        "[CB:smsService] call rejected - SMS service unavailable"))
                .onSuccess(event -> log.debug(
                        "[CB:smsService] call succeeded in {} ms",
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

        // Retry for Email Service calls
        Retry emailRetry = retryRegistry.retry("emailRetry");
        emailRetry.getEventPublisher()
                .onRetry(event -> log.warn(
                        "[Retry:email] Attempt {} for email service call",
                        event.getNumberOfRetryAttempts()))
                .onError(event -> log.error(
                        "[Retry:email] Failed after {} attempts",
                        event.getNumberOfRetryAttempts()));

        // Retry for SMS Service calls
        Retry smsRetry = retryRegistry.retry("smsRetry");
        smsRetry.getEventPublisher()
                .onRetry(event -> log.warn(
                        "[Retry:sms] Attempt {} for SMS service call",
                        event.getNumberOfRetryAttempts()))
                .onError(event -> log.error(
                        "[Retry:sms] Failed after {} attempts",
                        event.getNumberOfRetryAttempts()));
    }
}