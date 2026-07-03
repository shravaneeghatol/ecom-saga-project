package com.example.order.service;

import com.example.order.config.KafkaTopicConfig;
import com.example.order.dto.CreateOrderRequest;
import com.example.order.entity.Order;
import com.example.order.entity.OrderStatus;
import com.example.order.entity.OutboxEvent;
import com.example.order.event.SagaEvent;
import com.example.order.repository.OrderRepository;
import com.example.order.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public Order createOrder(CreateOrderRequest req) {
        Double amount = req.getAmount() != null ? req.getAmount() : 0.0;

        Order order = Order.builder()
                .customerId(req.getCustomerId())
                .productId(req.getProductId())
                .quantity(req.getQuantity())
                .amount(amount)
                .status(OrderStatus.CREATED)
                .totalAmount(amount)
                .build();
        order = orderRepository.save(order);

        Map<String, Object> payload = new HashMap<>();
        payload.put("orderId", order.getId().toString());
        payload.put("customerId", order.getCustomerId());
        payload.put("productId", order.getProductId());
        payload.put("quantity", order.getQuantity());
        payload.put("amount", order.getAmount());
        payload.put("totalAmount", order.getTotalAmount());

        saveOutbox(order.getId().toString(), "OrderCreated", KafkaTopicConfig.ORDER_EVENTS_TOPIC, payload);
        log.info("Order {} CREATED, OrderCreated event queued in outbox", order.getId());
        return order;
    }

    @Transactional
    public void cancelOrder(String orderId, String reason) {
        orderRepository.findById(UUID.fromString(orderId)).ifPresent(order -> {
            if (order.getStatus() == OrderStatus.CANCELLED) {
                log.info("Order {} already CANCELLED, ignoring duplicate compensation event", orderId);
                return;
            }
            order.setStatus(OrderStatus.CANCELLED);
            order.setFailureReason(reason);
            orderRepository.save(order);
            log.warn("Order {} CANCELLED (compensating). Reason: {}", orderId, reason);

            // Publish to failure topic via Outbox
            Map<String, Object> failurePayload = new HashMap<>();
            failurePayload.put("orderId", orderId);
            failurePayload.put("reason", reason);
            failurePayload.put("failureType", "BUSINESS");
            failurePayload.put("customerId", order.getCustomerId());
            failurePayload.put("productId", order.getProductId());
            failurePayload.put("amount", order.getAmount());

            saveOutbox(orderId, "OrderFailed", KafkaTopicConfig.ORDER_EVENTS_FAILURE_TOPIC, failurePayload);
            log.warn("[FAILURE-TOPIC] Order {} → order.events-failure | reason: {}", orderId, reason);
        });
    }

    @Transactional
    public void markCompleted(String orderId) {
        orderRepository.findById(UUID.fromString(orderId)).ifPresent(order -> {
            if (order.getStatus() == OrderStatus.CANCELLED) {
                log.warn("Order {} received success signal but was already CANCELLED - ignoring (out-of-order event)", orderId);
                return;
            }
            order.setStatus(OrderStatus.COMPLETED);
            orderRepository.save(order);
            log.info("✅ Order {} COMPLETED - every service in the saga succeeded", orderId);
        });
    }

    private void saveOutbox(String aggregateId, String eventType, String topic, Map<String, Object> payload) {
        try {
            SagaEvent event = SagaEvent.create(aggregateId, eventType, "order-service", payload);
            String json = objectMapper.writeValueAsString(event);
            OutboxEvent outbox = OutboxEvent.builder()
                    .aggregateId(aggregateId)
                    .eventType(eventType)
                    .topic(topic)
                    .payload(json)
                    .build();
            outboxEventRepository.save(outbox);
            log.debug("Outbox event saved: type={}, topic={}, aggregateId={}", eventType, topic, aggregateId);
        } catch (Exception e) {
            log.error("Failed to serialize saga event: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to serialize saga event", e);
        }
    }
}