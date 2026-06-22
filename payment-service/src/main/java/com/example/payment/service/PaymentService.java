package com.example.payment.service;

import com.example.payment.config.KafkaTopicConfig;
import com.example.payment.domain.*;
import com.example.payment.event.SagaEvent;
import com.example.payment.repository.OutboxEventRepository;
import com.example.payment.repository.PaymentRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    /**
     * PRIMARY saga step: charge the customer.
     * On success -> PaymentCompleted (forwarded payload + paymentId).
     * On business failure (declined) -> PaymentFailed (triggers inventory release + order cancel).
     */
    @Transactional
    public void processPayment(String orderId, Map<String, Object> incomingPayload, boolean simulateFailure) {
        Double amount = incomingPayload.get("amount") == null ? 0.0 : ((Number) incomingPayload.get("amount")).doubleValue();

        Payment payment = Payment.builder()
                .orderId(orderId)
                .amount(amount)
                .status(simulateFailure ? PaymentStatus.FAILED : PaymentStatus.COMPLETED)
                .build();
        payment = paymentRepository.save(payment);

        Map<String, Object> payload = new HashMap<>(incomingPayload);
        payload.put("paymentId", payment.getId().toString());

        if (simulateFailure) {
            payload.put("reason", "Payment declined (simulated)");
            saveOutbox(orderId, "PaymentFailed", KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, payload);
            log.warn("Payment FAILED for order {}", orderId);
        } else {
            saveOutbox(orderId, "PaymentCompleted", KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, payload);
            log.info("Payment COMPLETED for order {} (amount {})", orderId, amount);
        }
    }

    /** Compensation: a later step (notification) failed - refund this payment. */
    @Transactional
    public void refund(String orderId, String reason) {
        paymentRepository.findByOrderIdAndStatus(orderId, PaymentStatus.COMPLETED).ifPresent(p -> {
            p.setStatus(PaymentStatus.REFUNDED);
            paymentRepository.save(p);

            Map<String, Object> payload = new HashMap<>();
            payload.put("orderId", orderId);
            payload.put("paymentId", p.getId().toString());
            payload.put("reason", reason);
            saveOutbox(orderId, "PaymentRefunded", KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, payload);
            log.warn("Payment REFUNDED for order {} (compensating). Reason: {}", orderId, reason);
        });
    }

    public void publishFailure(String orderId, Map<String, Object> incomingPayload, String reason) {
        Map<String, Object> payload = new HashMap<>(incomingPayload);
        payload.put("reason", reason);
        saveOutbox(orderId, "PaymentFailed", KafkaTopicConfig.PAYMENT_EVENTS_TOPIC, payload);
    }

    private void saveOutbox(String aggregateId, String eventType, String topic, Map<String, Object> payload) {
        try {
            SagaEvent event = SagaEvent.create(aggregateId, eventType, "payment-service", payload);
            String json = objectMapper.writeValueAsString(event);
            OutboxEvent outbox = OutboxEvent.builder()
                    .aggregateId(aggregateId).eventType(eventType).topic(topic).payload(json).build();
            outboxEventRepository.save(outbox);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize saga event", e);
        }
    }
}
