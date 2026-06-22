package com.example.inventory.controller;

import com.example.inventory.domain.InventoryItem;
import com.example.inventory.domain.InventoryReservation;
import com.example.inventory.repository.InventoryItemRepository;
import com.example.inventory.repository.InventoryReservationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/inventory")
@RequiredArgsConstructor
public class InventoryController {

    private final InventoryItemRepository inventoryItemRepository;
    private final InventoryReservationRepository reservationRepository;

    @GetMapping("/items")
    public List<InventoryItem> items() {
        return inventoryItemRepository.findAll();
    }

    @GetMapping("/reservations")
    public List<InventoryReservation> reservations() {
        return reservationRepository.findAll();
    }
}
