package com.example.payment.repository;

import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface PaymentRepository extends JpaRepository<Payment, UUID> {
    Optional<Payment> findByOrderIdAndStatus(String orderId, PaymentStatus status);
}
