package com.example.inventory.service;

import com.example.inventory.domain.OutboxEvent;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

import static com.example.inventory.config.CircuitBreakerConfig.KAFKA_CB;

/**
 * Isolates the single blocking call to Kafka (kafkaTemplate.send(...).get(...))
 * behind the "kafkaPublisher" circuit breaker and retry mechanism.
 *
 * This is deliberately a SEPARATE bean from OutboxPublisher: Spring implements
 * @CircuitBreaker (like @Transactional, @Retryable, etc.) via a dynamic proxy
 * wrapped around the bean. Calling an annotated method from another method in
 * the SAME class ("self-invocation") bypasses that proxy entirely, silently
 * disabling the circuit breaker. Injecting this as a collaborator and calling
 * sendToKafka(...) on it goes through the real proxy, so OPEN/HALF_OPEN/CLOSED
 * state is actually enforced.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaEventSender {

    @Qualifier("stringKafkaTemplate")
    private final KafkaTemplate<String, String> kafkaTemplate;

    @CircuitBreaker(name = KAFKA_CB)
    @Retry(name = "kafkaRetry")
    public void sendToKafka(OutboxEvent event) throws Exception {
        log.debug("Sending event to Kafka: topic={}, key={}", event.getTopic(), event.getPartitionKey());
        kafkaTemplate.send(event.getTopic(), event.getPartitionKey(), event.getPayload())
                .get(10, TimeUnit.SECONDS);
        log.debug("Event sent successfully to Kafka");
    }
}