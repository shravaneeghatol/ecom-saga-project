package com.example.notification.service;

import com.example.notification.domain.OutboxEvent;
import com.example.notification.domain.OutboxStatus;
import com.example.notification.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaEventSender kafkaEventSender;

    @Qualifier("stringKafkaTemplate")
    private final KafkaTemplate<String, String> stringKafkaTemplate;

    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    private final Map<String, Boolean> processingCache = new ConcurrentHashMap<>();
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);

    @Scheduled(fixedDelay = 1500)
    @Transactional
    public void publishPendingEvents() {
        if (consecutiveFailures.get() > 10) {
            log.warn("[OUTBOX] Consecutive failures exceeded 10, backing off for 5 seconds");
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            consecutiveFailures.set(0);
            return;
        }

        try {
            List<OutboxEvent> events = outboxEventRepository
                    .findTop50ByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

            if (events.isEmpty()) {
                consecutiveFailures.set(0);
                return;
            }

            log.info("[OUTBOX] Processing {} pending events", events.size());

            int processed = 0;
            int failed = 0;

            for (OutboxEvent event : events) {
                String eventKey = event.getId().toString();

                if (processingCache.putIfAbsent(eventKey, Boolean.TRUE) != null) {
                    log.debug("[OUTBOX] Event {} already being processed, skipping", eventKey);
                    continue;
                }

                try {
                    if (event.getStatus() == OutboxStatus.PUBLISHED) {
                        log.warn("[OUTBOX] Event {} already published, but still in processing", eventKey);
                        processingCache.remove(eventKey);
                        continue;
                    }

                    kafkaEventSender.sendToKafka(event);

                    if (event.getStatus() != OutboxStatus.PUBLISHED) {
                        event.setStatus(OutboxStatus.PUBLISHED);
                        event.setPublishedAt(Instant.now());
                        outboxEventRepository.save(event);
                        processed++;

                        meterRegistry.counter("outbox.published",
                                "eventType", event.getEventType(),
                                "topic", event.getTopic()).increment();

                        log.info("[OUTBOX] published {} (orderId={}) -> topic '{}'",
                                event.getEventType(), event.getAggregateId(), event.getTopic());
                    }

                    consecutiveFailures.set(0);

                } catch (CallNotPermittedException open) {
                    log.warn("[OUTBOX] circuit breaker OPEN — Kafka appears DOWN. " +
                                    "Skipping {} remaining event(s). Will retry on next poll.",
                            events.size() - (processed + failed));

                    meterRegistry.counter("outbox.circuit_breaker_open").increment();
                    processingCache.remove(eventKey);
                    return;

                } catch (Exception ex) {
                    failed++;
                    event.setRetryCount(event.getRetryCount() + 1);

                    if (event.getRetryCount() > event.getMaxRetries()) {
                        log.error("[OUTBOX] event {} (orderId={}) exhausted {} retries → sending to DLT. Error: {}",
                                event.getId(), event.getAggregateId(), event.getMaxRetries(), ex.getMessage());

                        publishToDlt(event, ex.getMessage());
                        event.setStatus(OutboxStatus.FAILED);
                        event.setFailedAt(Instant.now());

                        meterRegistry.counter("outbox.failed",
                                "eventType", event.getEventType()).increment();
                    } else {
                        log.warn("[OUTBOX] event {} (orderId={}) failed (attempt {}/{}). Will retry. Error: {}",
                                event.getId(), event.getAggregateId(),
                                event.getRetryCount(), event.getMaxRetries(), ex.getMessage());

                        meterRegistry.counter("outbox.retry",
                                "attempt", String.valueOf(event.getRetryCount())).increment();
                    }

                    outboxEventRepository.save(event);
                    consecutiveFailures.incrementAndGet();
                } finally {
                    processingCache.remove(eventKey);
                }
            }

            if (processed > 0 || failed > 0) {
                log.info("[OUTBOX] Batch complete: {} published, {} failed, {} total",
                        processed, failed, events.size());
            }

        } catch (DataAccessException dae) {
            log.error("[OUTBOX] Database error processing outbox: {}", dae.getMessage());
            consecutiveFailures.incrementAndGet();
        } catch (Exception e) {
            log.error("[OUTBOX] Unexpected error processing outbox: {}", e.getMessage(), e);
            consecutiveFailures.incrementAndGet();
        }
    }

    private void publishToDlt(OutboxEvent event, String errorReason) {
        try {
            Map<String, Object> dltPayload = Map.of(
                    "originalTopic", event.getTopic(),
                    "originalPayload", event.getPayload(),
                    "aggregateId", event.getAggregateId(),
                    "eventType", event.getEventType(),
                    "retryCount", event.getRetryCount(),
                    "errorReason", errorReason != null ? errorReason : "unknown",
                    "failedAt", Instant.now().toString()
            );

            String dltJson = objectMapper.writeValueAsString(dltPayload);
            stringKafkaTemplate.send("notification.events-dlt", event.getAggregateId(), dltJson);

            log.error("[DLT] published failed event for orderId={} to notification.events-dlt",
                    event.getAggregateId());

            meterRegistry.counter("outbox.dlt_published").increment();

        } catch (Exception dltEx) {
            log.error("[DLT] CRITICAL — could not publish to DLT for orderId={}: {}",
                    event.getAggregateId(), dltEx.getMessage());

            meterRegistry.counter("outbox.dlt_failed").increment();
        }
    }
}