package com.example.inventory.listener;

import com.example.inventory.event.SagaEvent;
import com.example.inventory.service.InventoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class SagaEventListener {

    private final InventoryService inventoryService;

    // ---------------------------------------------------------------
    // PRIMARY listener: order.events (this service is the first hop after order creation)
    // 4 attempts total (1 initial + 3 retries), exponential backoff, then -> order.events-dlt
    // Creates: order.events-retry-0, -retry-1, -retry-2, -dlt
    // ---------------------------------------------------------------
    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 15000),
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            dltStrategy = DltStrategy.ALWAYS_RETRY_ON_ERROR,
            include = { RuntimeException.class }
    )
    @KafkaListener(topics = "order.events",
            groupId = "inventory-service-group",
            containerFactory = "kafkaListenerContainerFactory" )
    public void onOrderCreated(SagaEvent event) {
        if (!"OrderCreated".equals(event.getEventType())) return;

        String orderId = event.getSagaId();
        String simulateAt = (String) event.getPayload().get("simulateTransientError");
        if ("inventory".equalsIgnoreCase(simulateAt)) {
            log.warn("Simulating TRANSIENT failure for order {} in inventory-service (will retry x3 then go to DLT)", orderId);
            throw new RuntimeException("Simulated transient error in inventory-service");
        }

        boolean simulateBusinessFailure = Boolean.TRUE.equals(event.getPayload().get("simulateInventoryFailure"));
        inventoryService.reserve(orderId, event.getPayload(), simulateBusinessFailure);
    }

    @DltHandler
    public void handleOrderDlt(SagaEvent event) {
        log.error("[DLT] order.events exhausted retries for order {}", event.getSagaId());
        inventoryService.publishFailure(event.getSagaId(), event.getPayload(), "Inventory processing failed after max retries (DLT)");
    }

    // ---------------------------------------------------------------
    // Compensation listener: payment.events (release stock if payment fails)
    // ---------------------------------------------------------------
    @KafkaListener(topics = "payment.events", groupId = "inventory-service-compensation-group")
    public void onPaymentEvent(SagaEvent event) {
        if ("PaymentFailed".equals(event.getEventType())) {
            String reason = String.valueOf(event.getPayload().getOrDefault("reason", "payment failed"));
            inventoryService.releaseReservation(event.getSagaId(), reason);
        }
    }

    // ---------------------------------------------------------------
    // Compensation listener: notification.events (release stock if notification ultimately fails)
    // ---------------------------------------------------------------
    @KafkaListener(topics = "notification.events", groupId = "inventory-service-compensation-group")
    public void onNotificationEvent(SagaEvent event) {
        if ("NotificationFailed".equals(event.getEventType())) {
            inventoryService.releaseReservation(event.getSagaId(), "Compensating due to notification failure");
        }
    }
}
