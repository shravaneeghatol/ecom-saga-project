package com.example.inventory.controller;

import com.example.inventory.domain.InventoryItem;
import com.example.inventory.domain.InventoryReservation;
import com.example.inventory.repository.InventoryItemRepository;
import com.example.inventory.repository.InventoryReservationRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/inventory")
@RequiredArgsConstructor
@Slf4j
public class InventoryController {

    private final InventoryItemRepository inventoryItemRepository;
    private final InventoryReservationRepository reservationRepository;
    private final MeterRegistry meterRegistry;

    @GetMapping("/items")
    public List<InventoryItem> items() {
        return inventoryItemRepository.findAll();
    }

    @GetMapping("/reservations")
    public List<InventoryReservation> reservations() {
        return reservationRepository.findAll();
    }

    @GetMapping("/items/{productId}")
    public ResponseEntity<InventoryItem> getItem(@PathVariable String productId) {
        return inventoryItemRepository.findByProductId(productId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        // Check database connectivity
        try {
            long count = inventoryItemRepository.count();
            return ResponseEntity.ok(Map.of(
                    "status", "UP",
                    "itemsCount", String.valueOf(count),
                    "service", "inventory-service"
            ));
        } catch (Exception e) {
            log.error("Health check failed: {}", e.getMessage());
            return ResponseEntity.status(503).body(Map.of(
                    "status", "DOWN",
                    "error", e.getMessage()
            ));
        }
    }

    @GetMapping("/metrics")
    public ResponseEntity<Map<String, Object>> metrics() {
        return ResponseEntity.ok(Map.of(
                "totalItems", inventoryItemRepository.count(),
                "totalReservations", reservationRepository.count(),
                "service", "inventory-service"
        ));
    }

    @PostMapping("/items")
    public ResponseEntity<InventoryItem> createItem(@RequestBody InventoryItem item) {
        InventoryItem saved = inventoryItemRepository.save(item);
        meterRegistry.counter("inventory.item.created").increment();
        return ResponseEntity.ok(saved);
    }
}