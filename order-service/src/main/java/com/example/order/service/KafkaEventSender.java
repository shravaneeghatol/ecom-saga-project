package com.example.order.service;

import com.example.order.entity.OutboxEvent;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

import static com.example.order.config.CircuitBreakerConfig.KAFKA_PUBLISHER_BREAKER;

/**
 * Isolates the single blocking Kafka send behind the "kafkaPublisher" circuit breaker.
 *
 * WHY A SEPARATE BEAN?
 * ─────────────────────
 * Spring implements @CircuitBreaker (like @Transactional) via a JDK/CGLIB proxy
 * that wraps the bean.  If you call an @CircuitBreaker method from another method
 * in the SAME class ("self-invocation"), the call goes directly to `this` — not
 * through the proxy — so the circuit breaker is silently bypassed.
 *
 * By keeping this in its own bean (injected into OutboxPublisher), every call to
 * sendToKafka() passes through the Resilience4j proxy and the breaker's state
 * (CLOSED / OPEN / HALF_OPEN) is enforced correctly.
 *
 * HOW IT WORKS WITH THE OUTBOX
 * ─────────────────────────────
 *  OutboxPublisher (scheduled every 1.5s)
 *       │
 *       │  calls per PENDING outbox row
 *       ▼
 *  KafkaEventSender.sendToKafka(event)   ← @CircuitBreaker wraps this
 *       │
 *       │  if CLOSED / HALF_OPEN
 *       ▼
 *  kafkaTemplate.send(...).get(10s)      ← blocking send to broker
 *       │
 *       │  if breaker trips OPEN
 *       ▼
 *  CallNotPermittedException thrown       ← OutboxPublisher catches this,
 *                                           stops the current batch, rows
 *                                           stay PENDING for next poll tick
 *
 * NO FALLBACK METHOD HERE.
 * The caller (OutboxPublisher) explicitly catches CallNotPermittedException and
 * any other exception itself, so adding a fallback on this method would hide
 * those exceptions and break OutboxPublisher's logic.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class KafkaEventSender {

    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Publishes a single outbox event to Kafka.
     *
     * Throws:
     *  - CallNotPermittedException  when the breaker is OPEN (fast-fail, no network call)
     *  - ExecutionException         when the broker rejects / times out (real failure)
     *  - TimeoutException           when .get(10s) exceeds the wait limit
     *
     * Both failure types are recorded by the breaker and count toward the failure rate.
     */
    @CircuitBreaker(name = KAFKA_PUBLISHER_BREAKER)
    public void sendToKafka(OutboxEvent event) throws Exception {
        log.debug("[KAFKA-SENDER] Sending event {} topic='{}' key='{}'",
                event.getEventType(), event.getTopic(), event.getPartitionKey());

        kafkaTemplate.send(event.getTopic(), event.getPartitionKey(), event.getPayload())
                .get(10, TimeUnit.SECONDS);  // blocking — intentional; CB guards against pile-up
    }
}