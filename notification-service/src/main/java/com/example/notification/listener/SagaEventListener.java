package com.example.notification.listener;

import com.example.notification.event.SagaEvent;
import com.example.notification.service.NotificationService;
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

    private final NotificationService notificationService;

    // ---------------------------------------------------------------
    // PRIMARY listener: payment.events (final step of the happy path)
    // Creates: payment.events-retry-0, -retry-1, -retry-2, -dlt
    // No compensation listeners needed here - nothing sits downstream
    // of notification-service in the saga.
    // ---------------------------------------------------------------
    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 15000),
            retryTopicSuffix = "-retry",
            dltTopicSuffix = "-dlt",
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            include = { RuntimeException.class }
    )
    @KafkaListener(topics = "payment.events", groupId = "notification-service-group")
    public void onPaymentCompleted(SagaEvent event) {
        if (!"PaymentCompleted".equals(event.getEventType())) return;

        String orderId = event.getSagaId();
        String simulateAt = (String) event.getPayload().get("simulateTransientError");
        if ("notification".equalsIgnoreCase(simulateAt)) {
            log.warn("Simulating TRANSIENT failure for order {} in notification-service (will retry x3 then go to DLT)", orderId);
            throw new RuntimeException("Simulated transient error in notification-service");
        }

        notificationService.sendNotification(orderId, event.getPayload());
    }

    @DltHandler
    public void handlePaymentDlt(SagaEvent event) {
        log.error("[DLT] payment.events exhausted retries for order {}", event.getSagaId());
        notificationService.publishFailure(event.getSagaId(), event.getPayload(), "Notification failed after max retries (DLT)");
    }
}
