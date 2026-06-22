package com.example.inventory.service;

import com.example.inventory.domain.InventoryItem;
import com.example.inventory.repository.InventoryItemRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/** Seeds a couple of demo products on startup so you can test both success and "insufficient stock" paths. */
@Component
@RequiredArgsConstructor
public class InventoryDataSeeder implements CommandLineRunner {

    private final InventoryItemRepository inventoryItemRepository;

    @Override
    public void run(String... args) {
        if (inventoryItemRepository.count() == 0) {
            inventoryItemRepository.save(InventoryItem.builder().productId("PROD-1").availableQty(100).reservedQty(0).build());
            inventoryItemRepository.save(InventoryItem.builder().productId("PROD-2").availableQty(5).reservedQty(0).build());
            inventoryItemRepository.save(InventoryItem.builder().productId("PROD-OUT-OF-STOCK").availableQty(0).reservedQty(0).build());
        }
    }
}
