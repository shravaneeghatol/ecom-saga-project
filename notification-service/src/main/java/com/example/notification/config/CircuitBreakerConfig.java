package com.example.notification.config;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;

/**
 * Wires up logging for the "kafkaPublisher" circuit breaker (declared in
 * application.yml under resilience4j.circuitbreaker.instances.kafkaPublisher).
 *
 * The breaker protects OutboxPublisher's call to KafkaTemplate.send(...).get(...).
 * That call is the ONLY blocking call to an external system in this service
 * (every other write is to the local H2 DB inside the same transaction),
 * so it's the one place a circuit breaker is meaningful here:
 *
 *  - CLOSED:    normal operation, every PENDING outbox row is sent to Kafka.
 *  - OPEN:      Kafka has been failing/timing out beyond the configured
 *               threshold -> publishing is skipped immediately (no blocking
 *               .get(10s) calls pile up) and rows are left PENDING so the
 *               next scheduled poll (every 1.5s) picks them up again.
 *  - HALF_OPEN: after the wait duration, a handful of trial calls are let
 *               through to see if Kafka has recovered.
 */
@Configuration
@RequiredArgsConstructor
@Slf4j
public class CircuitBreakerConfig {

    public static final String KAFKA_PUBLISHER_BREAKER = "kafkaPublisher";

    private final CircuitBreakerRegistry circuitBreakerRegistry;

    @PostConstruct
    public void registerEventListeners() {
        CircuitBreaker breaker = circuitBreakerRegistry.circuitBreaker(KAFKA_PUBLISHER_BREAKER);

        breaker.getEventPublisher()
                .onStateTransition(event -> log.warn(
                        "[CIRCUIT-BREAKER:{}] state transition: {} -> {}",
                        KAFKA_PUBLISHER_BREAKER,
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()))
                .onError(event -> log.error(
                        "[CIRCUIT-BREAKER:{}] call failed after {} ms: {}",
                        KAFKA_PUBLISHER_BREAKER,
                        event.getElapsedDuration().toMillis(),
                        event.getThrowable() == null ? "n/a" : event.getThrowable().toString()))
                .onCallNotPermitted(event -> log.warn(
                        "[CIRCUIT-BREAKER:{}] call rejected - breaker is OPEN, failing fast",
                        KAFKA_PUBLISHER_BREAKER))
                .onSuccess(event -> log.debug(
                        "[CIRCUIT-BREAKER:{}] call succeeded in {} ms",
                        KAFKA_PUBLISHER_BREAKER,
                        event.getElapsedDuration().toMillis()));
    }
}
