package com.example.payment.listener;

import com.example.payment.event.SagaEvent;
import com.example.payment.service.PaymentService;
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

    private final PaymentService paymentService;

    // ---------------------------------------------------------------
    // PRIMARY listener: inventory.events
    // Creates: inventory.events-retry-0, -retry-1, -retry-2, -dlt
    // ---------------------------------------------------------------
    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 15000),
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = { RuntimeException.class }
    )
    @KafkaListener(topics = "inventory.events", groupId = "payment-service-group")
    public void onInventoryReserved(SagaEvent event) {
        if (!"InventoryReserved".equals(event.getEventType())) return;

        String orderId = event.getSagaId();
        String simulateAt = (String) event.getPayload().get("simulateTransientError");
        if ("payment".equalsIgnoreCase(simulateAt)) {
            log.warn("Simulating TRANSIENT failure for order {} in payment-service (will retry x3 then go to DLT)", orderId);
            throw new RuntimeException("Simulated transient error in payment-service");
        }

        boolean simulateBusinessFailure = Boolean.TRUE.equals(event.getPayload().get("simulatePaymentFailure"));
        paymentService.processPayment(orderId, event.getPayload(), simulateBusinessFailure);
    }

    @DltHandler
    public void handleInventoryDlt(SagaEvent event) {
        log.error("[DLT] inventory.events exhausted retries for order {}", event.getSagaId());
        paymentService.publishFailure(event.getSagaId(), event.getPayload(), "Payment processing failed after max retries (DLT)");
    }

    // ---------------------------------------------------------------
    // Compensation listener: notification.events (refund if notification ultimately fails)
    // ---------------------------------------------------------------
    @KafkaListener(topics = "notification.events", groupId = "payment-service-compensation-group")
    public void onNotificationEvent(SagaEvent event) {
        if ("NotificationFailed".equals(event.getEventType())) {
            paymentService.refund(event.getSagaId(), "Compensating due to notification failure");
        }
    }
}
