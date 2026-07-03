package com.example.payment.controller;

import com.example.payment.domain.Payment;
import com.example.payment.dto.PaymentRequest;
import com.example.payment.dto.PaymentResponse;
import com.example.payment.repository.PaymentRepository;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
@Slf4j
public class PaymentController {

    private final PaymentRepository paymentRepository;
    private final MeterRegistry meterRegistry;

    @GetMapping
    public List<Payment> all() {
        return paymentRepository.findAll();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Payment> get(@PathVariable String id) {
        return paymentRepository.findById(UUID.fromString(id))
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<List<Payment>> getByOrderId(@PathVariable String orderId) {
        List<Payment> payments = paymentRepository.findByOrderId(orderId);
        return ResponseEntity.ok(payments);
    }

    @GetMapping("/status/{orderId}")
    public ResponseEntity<PaymentResponse> getPaymentStatus(@PathVariable String orderId) {
        Optional<Payment> payment = paymentRepository.findByOrderIdAndStatus(
                orderId, com.example.payment.domain.PaymentStatus.COMPLETED);

        if (payment.isPresent()) {
            return ResponseEntity.ok(PaymentResponse.builder()
                    .success(true)
                    .transactionId(payment.get().getId().toString())
                    .message("Payment completed")
                    .build());
        } else {
            Optional<Payment> failedPayment = paymentRepository.findByOrderIdAndStatus(
                    orderId, com.example.payment.domain.PaymentStatus.FAILED);

            if (failedPayment.isPresent()) {
                return ResponseEntity.ok(PaymentResponse.builder()
                        .success(false)
                        .message("Payment failed")
                        .build());
            } else {
                return ResponseEntity.status(404).body(PaymentResponse.builder()
                        .success(false)
                        .message("Payment not found for order: " + orderId)
                        .build());
            }
        }
    }

    @PostMapping("/process")
    public ResponseEntity<PaymentResponse> processPayment(@RequestBody PaymentRequest request) {
        log.info("Processing payment for order: {}", request.getOrderId());

        try {
            // Simple payment processing with business rules
            if (request.getAmount() <= 0) {
                return ResponseEntity.badRequest().body(PaymentResponse.builder()
                        .success(false)
                        .message("Invalid payment amount")
                        .build());
            }

            // Business rule: Amount limit check
            if (request.getAmount() > 10000) {
                return ResponseEntity.badRequest().body(PaymentResponse.builder()
                        .success(false)
                        .message("Amount exceeds limit: " + request.getAmount())
                        .build());
            }

            // Business rule: Blacklist check
            if ("BLACKLISTED".equals(request.getCustomerId())) {
                return ResponseEntity.badRequest().body(PaymentResponse.builder()
                        .success(false)
                        .message("Customer is blacklisted")
                        .build());
            }

            Payment payment = Payment.builder()
                    .orderId(request.getOrderId())
                    .amount(request.getAmount())
                    .status(com.example.payment.domain.PaymentStatus.COMPLETED)
                    .build();
            payment = paymentRepository.save(payment);

            meterRegistry.counter("payment.processed",
                    "status", "success").increment();

            return ResponseEntity.ok(PaymentResponse.builder()
                    .success(true)
                    .transactionId(payment.getId().toString())
                    .message("Payment processed successfully")
                    .build());

        } catch (Exception e) {
            log.error("Error processing payment: {}", e.getMessage(), e);
            meterRegistry.counter("payment.processed",
                    "status", "failed").increment();

            return ResponseEntity.internalServerError().body(PaymentResponse.builder()
                    .success(false)
                    .message("Payment processing failed: " + e.getMessage())
                    .build());
        }
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        try {
            long count = paymentRepository.count();
            return ResponseEntity.ok(Map.of(
                    "status", "UP",
                    "paymentsCount", String.valueOf(count),
                    "service", "payment-service"
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
        long total = paymentRepository.count();
        long completed = paymentRepository.countByStatus(com.example.payment.domain.PaymentStatus.COMPLETED);
        long failed = paymentRepository.countByStatus(com.example.payment.domain.PaymentStatus.FAILED);
        long refunded = paymentRepository.countByStatus(com.example.payment.domain.PaymentStatus.REFUNDED);

        return ResponseEntity.ok(Map.of(
                "totalPayments", total,
                "completed", completed,
                "failed", failed,
                "refunded", refunded,
                "service", "payment-service"
        ));
    }

    @PostMapping("/compensate/{orderId}")
    public ResponseEntity<PaymentResponse> compensatePayment(@PathVariable String orderId) {
        log.info("Manually compensating payment for order: {}", orderId);

        try {
            Optional<Payment> payment = paymentRepository.findByOrderIdAndStatus(
                    orderId, com.example.payment.domain.PaymentStatus.COMPLETED);

            if (payment.isEmpty()) {
                return ResponseEntity.notFound().build();
            }

            payment.get().setStatus(com.example.payment.domain.PaymentStatus.REFUNDED);
            paymentRepository.save(payment.get());

            meterRegistry.counter("payment.compensated").increment();

            return ResponseEntity.ok(PaymentResponse.builder()
                    .success(true)
                    .message("Payment compensated successfully")
                    .build());

        } catch (Exception e) {
            log.error("Error compensating payment: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(PaymentResponse.builder()
                    .success(false)
                    .message("Compensation failed: " + e.getMessage())
                    .build());
        }
    }

    @PostMapping("/refund/{orderId}")
    public ResponseEntity<PaymentResponse> refundPayment(@PathVariable String orderId) {
        log.info("Manually refunding payment for order: {}", orderId);
        return compensatePayment(orderId);
    }
}