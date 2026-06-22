package com.example.order.listener;

import com.example.order.event.SagaEvent;
import com.example.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Component;

/**
 * order-service reacts to events from every other service:
 *  - inventory.events / payment.events : COMPENSATION ONLY (cancel the order on failure)
 *  - notification.events               : PRIMARY processing - this is the final saga step.
 *                                         order-service "owns" this topic's retry/DLT set because
 *                                         it's the natural place to confirm overall saga success.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SagaEventListener {

    private final OrderService orderService;

    // ---------------------------------------------------------------
    // Compensation listener: inventory.events
    // ---------------------------------------------------------------
    @KafkaListener(topics = "inventory.events", groupId = "order-service-compensation-group")
    public void onInventoryEvent(SagaEvent event) {
        if ("InventoryReservationFailed".equals(event.getEventType())) {
            String reason = String.valueOf(event.getPayload().getOrDefault("reason", "inventory reservation failed"));
            orderService.cancelOrder(event.getSagaId(), reason);
        }
        // InventoryReserved / InventoryReleased -> no action needed here
    }

    // ---------------------------------------------------------------
    // Compensation listener: payment.events
    // ---------------------------------------------------------------
    @KafkaListener(topics = "payment.events", groupId = "order-service-compensation-group")
    public void onPaymentEvent(SagaEvent event) {
        if ("PaymentFailed".equals(event.getEventType())) {
            String reason = String.valueOf(event.getPayload().getOrDefault("reason", "payment failed"));
            orderService.cancelOrder(event.getSagaId(), reason);
        }
    }

    // ---------------------------------------------------------------
    // PRIMARY listener: notification.events
    // 4 attempts total (1 initial + 3 retries) with exponential backoff,
    // then routed to notification.events-dlt.
    // Creates: notification.events-retry-0, -retry-1, -retry-2, -dlt
    // ---------------------------------------------------------------
    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 15000),
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = { RuntimeException.class }
    )
    @KafkaListener(topics = "notification.events", groupId = "order-service-group")
    public void onNotificationEvent(SagaEvent event) {
        if ("NotificationSent".equals(event.getEventType())) {
            orderService.markCompleted(event.getSagaId());
        } else if ("NotificationFailed".equals(event.getEventType())) {
            String reason = String.valueOf(event.getPayload().getOrDefault("reason", "notification failed"));
            orderService.cancelOrder(event.getSagaId(), reason);
        }
    }

    @DltHandler
    public void handleNotificationDlt(SagaEvent event) {
        log.error("[DLT] notification.events exhausted retries for order {}", event.getSagaId());
        orderService.cancelOrder(event.getSagaId(), "Notification step failed after max retries (DLT)");
    }
}
