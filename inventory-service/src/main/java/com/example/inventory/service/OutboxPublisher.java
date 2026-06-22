package com.example.inventory.service;

import com.example.inventory.domain.OutboxEvent;
import com.example.inventory.domain.OutboxStatus;
import com.example.inventory.repository.OutboxEventRepository;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * THE OUTBOX PATTERN POLLER.
 * Runs on a fixed schedule, finds PENDING rows written by business transactions,
 * and publishes them to Kafka. This decouples the local DB commit from the Kafka
 * publish, so a saga step is never "half done" (DB committed but event lost).
 *
 * CIRCUIT BREAKER: the only blocking external call in this service is the
 * Kafka send (delegated to KafkaEventSender, which wraps it with
 * kafkaTemplate.send(...).get(10, SECONDS)). If the broker is down or slow,
 * that .get() blocks for up to 10s PER MESSAGE - with 50 pending rows that's
 * up to 500s of a scheduler thread doing nothing useful, every 1.5s tick.
 * The "kafkaPublisher" circuit breaker (config in application.yml) watches
 * the failure rate of KafkaEventSender.sendToKafka(...) and, once it trips
 * OPEN, calls are rejected immediately (CallNotPermittedException) instead
 * of blocking - the batch is abandoned for this tick and every remaining
 * row simply stays PENDING, to be retried on the next poll once the
 * breaker lets calls through again (HALF_OPEN -> CLOSED).
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaEventSender kafkaEventSender;

    @Scheduled(fixedDelay = 1500)
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> events = outboxEventRepository.findTop50ByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

        for (int i = 0; i < events.size(); i++) {
            OutboxEvent event = events.get(i);
            try {
                kafkaEventSender.sendToKafka(event);
                event.setStatus(OutboxStatus.PUBLISHED);
                event.setPublishedAt(Instant.now());
                outboxEventRepository.save(event);
                log.info("[OUTBOX] published {} (orderId={}) -> topic '{}'",
                        event.getEventType(), event.getAggregateId(), event.getTopic());
            } catch (CallNotPermittedException open) {
                // Breaker is OPEN: stop hammering Kafka for the rest of this batch too.
                // Every row from here on is left PENDING (untouched) so the next
                // scheduled poll retries it once the breaker allows calls again.
                log.warn("[OUTBOX] circuit breaker OPEN - skipping remaining {} pending event(s) this tick, " +
                                "will retry on next poll. First skipped: {} (orderId={})",
                        events.size() - i, event.getEventType(), event.getAggregateId());
                return;
            } catch (Exception ex) {
                // Real failure (serialization issue, broker rejected the record, etc).
                // Leave it PENDING too - this repo's poller only ever re-reads PENDING rows,
                // so marking it FAILED here would mean it's silently dropped forever.
                log.error("[OUTBOX] failed to publish event {} (orderId={}): {}. Will retry on next poll.",
                        event.getId(), event.getAggregateId(), ex.getMessage());
            }
        }
    }
}
