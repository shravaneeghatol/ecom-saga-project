package com.example.inventory.repository;

import com.example.inventory.domain.InventoryReservation;
import com.example.inventory.domain.ReservationStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface InventoryReservationRepository extends JpaRepository<InventoryReservation, UUID> {
    Optional<InventoryReservation> findByOrderIdAndStatus(String orderId, ReservationStatus status);
}
