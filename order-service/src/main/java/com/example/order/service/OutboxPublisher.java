package com.example.order.service;

import com.example.order.entity.OutboxEvent;
import com.example.order.entity.OutboxStatus;
import com.example.order.repository.OutboxEventRepository;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * THE OUTBOX PATTERN POLLER — wired with the Kafka Circuit Breaker.
 *
 * Runs on a fixed 1.5-second delay, reads up to 50 PENDING outbox rows
 * and ships each one to Kafka via KafkaEventSender (which has the
 * @CircuitBreaker annotation on its sendToKafka method).
 *
 * ── Normal flow (breaker CLOSED) ───────────────────────────────────────
 *   1. Read PENDING rows from outbox table.
 *   2. For each row → KafkaEventSender.sendToKafka() → blocking .get(10s).
 *   3. On success  → mark PUBLISHED + record publishedAt.
 *   4. On real failure (broker down, serialization error)
 *      → log error, leave as PENDING → retried on next poll tick.
 *
 * ── Breaker OPEN ────────────────────────────────────────────────────────
 *   sendToKafka() throws CallNotPermittedException immediately (no network call).
 *   We catch it, log a single warning, and RETURN early — skipping the rest
 *   of the batch.  All rows remain PENDING so they are retried once the
 *   breaker transitions HALF_OPEN → CLOSED (controlled by application.yml:
 *   wait-duration-in-open-state + automatic-transition-from-open-to-half-open).
 *
 * ── Why we stop the whole batch when the breaker is OPEN ────────────────
 *   If the broker is unreachable, continuing to loop over 50 rows would
 *   either: (a) burn through 50 × 10s = 500s of scheduler thread time
 *   waiting for .get() to time out, or (b) trigger 50 more CallNotPermitted
 *   exceptions in a row with no useful result.  Stopping early is cheaper.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OutboxPublisher {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaEventSender kafkaEventSender;  // has @CircuitBreaker on sendToKafka

    @Scheduled(fixedDelay = 1500)
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> events =
                outboxEventRepository.findTop50ByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

        if (events.isEmpty()) return;

        log.debug("[OUTBOX] Found {} PENDING event(s) to publish", events.size());

        for (int i = 0; i < events.size(); i++) {
            OutboxEvent event = events.get(i);
            try {
                // ── This call goes through the Resilience4j proxy ──────────
                kafkaEventSender.sendToKafka(event);

                // ── Success ────────────────────────────────────────────────
                event.setStatus(OutboxStatus.PUBLISHED);
                event.setPublishedAt(Instant.now());
                outboxEventRepository.save(event);
                log.info("[OUTBOX] ✅ PUBLISHED: type={} orderId={} topic='{}'",
                        event.getEventType(), event.getAggregateId(), event.getTopic());

            } catch (CallNotPermittedException open) {
                // ── Breaker is OPEN → fast-fail, stop the entire batch ─────
                // Remaining rows are untouched (still PENDING).
                // The next scheduled tick will retry once the breaker allows calls.
                log.warn("[OUTBOX] 🚫 Circuit breaker OPEN — aborting batch. " +
                                "Skipping {} remaining event(s). Will retry on next poll. " +
                                "First skipped: type={} orderId={}",
                        events.size() - i,
                        event.getEventType(),
                        event.getAggregateId());
                return;  // ← key: stop processing, leave all remaining rows PENDING

            } catch (Exception ex) {
                // ── Real failure (broker rejected record, serialization issue, etc.) ──
                // Leave this row PENDING too — the poller only ever re-reads PENDING rows
                // so it will be retried on the next tick automatically.
                // We do NOT mark it FAILED here because that would silently drop it.
                log.error("[OUTBOX] ❌ FAILED to publish event id={} type={} orderId={}: {}. " +
                                "Leaving PENDING for retry on next poll.",
                        event.getId(), event.getEventType(), event.getAggregateId(),
                        ex.getMessage());
            }
        }
    }
}