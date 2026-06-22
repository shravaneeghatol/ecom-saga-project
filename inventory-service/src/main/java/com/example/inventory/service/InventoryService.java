package com.example.inventory.service;

import com.example.inventory.config.KafkaTopicConfig;
import com.example.inventory.domain.*;
import com.example.inventory.event.SagaEvent;
import com.example.inventory.repository.InventoryItemRepository;
import com.example.inventory.repository.InventoryReservationRepository;
import com.example.inventory.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryService {

    private final InventoryItemRepository inventoryItemRepository;
    private final InventoryReservationRepository reservationRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    /**
     * PRIMARY saga step: try to reserve stock for this order.
     * On success -> InventoryReserved (forwarded payload + reservationId).
     * On business failure (no stock) -> InventoryReservationFailed (triggers order-service compensation).
     */
    @Transactional
    public void reserve(String orderId, Map<String, Object> incomingPayload, boolean simulateFailure) {
        String productId = (String) incomingPayload.get("productId");
        Integer qty = (Integer) incomingPayload.get("quantity");

        Optional<InventoryItem> itemOpt = inventoryItemRepository.findByProductId(productId);
        boolean insufficientStock = simulateFailure || itemOpt.isEmpty() || itemOpt.get().getAvailableQty() < qty;

        if (insufficientStock) {
            InventoryReservation r = InventoryReservation.builder()
                    .orderId(orderId).productId(productId).quantity(qty)
                    .status(ReservationStatus.FAILED).build();
            reservationRepository.save(r);
            publishFailure(orderId, incomingPayload, "Insufficient stock for product " + productId);
            log.warn("Reservation FAILED for order {} (product {})", orderId, productId);
            return;
        }

        InventoryItem item = itemOpt.get();
        item.setAvailableQty(item.getAvailableQty() - qty);
        item.setReservedQty(item.getReservedQty() + qty);
        inventoryItemRepository.save(item);

        InventoryReservation r = InventoryReservation.builder()
                .orderId(orderId).productId(productId).quantity(qty)
                .status(ReservationStatus.RESERVED).build();
        r = reservationRepository.save(r);

        Map<String, Object> payload = new HashMap<>(incomingPayload);
        payload.put("reservationId", r.getId().toString());
        saveOutbox(orderId, "InventoryReserved", KafkaTopicConfig.INVENTORY_EVENTS_TOPIC, payload);
        log.info("Reservation RESERVED for order {} (product {}, qty {})", orderId, productId, qty);
    }

    /** Compensation: payment (or a later step) failed - release stock we previously reserved. */
    @Transactional
    public void releaseReservation(String orderId, String reason) {
        reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatus.RESERVED).ifPresent(r -> {
            inventoryItemRepository.findByProductId(r.getProductId()).ifPresent(item -> {
                item.setAvailableQty(item.getAvailableQty() + r.getQuantity());
                item.setReservedQty(Math.max(0, item.getReservedQty() - r.getQuantity()));
                inventoryItemRepository.save(item);
            });
            r.setStatus(ReservationStatus.RELEASED);
            reservationRepository.save(r);

            Map<String, Object> payload = new HashMap<>();
            payload.put("orderId", orderId);
            payload.put("reservationId", r.getId().toString());
            payload.put("reason", reason);
            saveOutbox(orderId, "InventoryReleased", KafkaTopicConfig.INVENTORY_EVENTS_TOPIC, payload);
            log.warn("Reservation RELEASED for order {} (compensating). Reason: {}", orderId, reason);
        });
    }

    public void publishFailure(String orderId, Map<String, Object> incomingPayload, String reason) {
        Map<String, Object> payload = new HashMap<>(incomingPayload);
        payload.put("reason", reason);
        saveOutbox(orderId, "InventoryReservationFailed", KafkaTopicConfig.INVENTORY_EVENTS_TOPIC, payload);
    }

    private void saveOutbox(String aggregateId, String eventType, String topic, Map<String, Object> payload) {
        try {
            SagaEvent event = SagaEvent.create(aggregateId, eventType, "inventory-service", payload);
            String json = objectMapper.writeValueAsString(event);
            OutboxEvent outbox = OutboxEvent.builder()
                    .aggregateId(aggregateId).eventType(eventType).topic(topic).payload(json).build();
            outboxEventRepository.save(outbox);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize saga event", e);
        }
    }
}
