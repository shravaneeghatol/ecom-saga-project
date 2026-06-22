package com.example.payment.service;

import com.example.payment.domain.OutboxEvent;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

import static com.example.payment.config.CircuitBreakerConfig.KAFKA_PUBLISHER_BREAKER;

/**
 * Isolates the single blocking call to Kafka (kafkaTemplate.send(...).get(...))
 * behind the "kafkaPublisher" circuit breaker.
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
public class KafkaEventSender {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @CircuitBreaker(name = KAFKA_PUBLISHER_BREAKER)
    public void sendToKafka(OutboxEvent event) throws Exception {
        kafkaTemplate.send(event.getTopic(), event.getPartitionKey(), event.getPayload())
                .get(10, TimeUnit.SECONDS);
    }
}
