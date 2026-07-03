package com.example.notification.controller;

import com.example.notification.domain.Notification;
import com.example.notification.domain.NotificationStatus;
import com.example.notification.dto.EmailRequest;
import com.example.notification.dto.SmsRequest;
import com.example.notification.repository.NotificationRepository;
import com.example.notification.service.NotificationService;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
@Slf4j
public class NotificationController {

    private final NotificationRepository notificationRepository;
    private final NotificationService notificationService;
    private final MeterRegistry meterRegistry;

    @GetMapping
    public List<Notification> all() {
        return notificationRepository.findAll();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Notification> get(@PathVariable String id) {
        return notificationRepository.findById(UUID.fromString(id))
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/order/{orderId}")
    public ResponseEntity<List<Notification>> getByOrderId(@PathVariable String orderId) {
        // Find all notifications for an order
        List<Notification> notifications = notificationRepository.findAll().stream()
                .filter(n -> n.getOrderId().equals(orderId))
                .toList();
        return ResponseEntity.ok(notifications);
    }

    @GetMapping("/status/{orderId}")
    public ResponseEntity<Map<String, Object>> getNotificationStatus(@PathVariable String orderId) {
        var sent = notificationRepository.findByOrderIdAndStatus(orderId, NotificationStatus.SENT);
        var partial = notificationRepository.findByOrderIdAndStatus(orderId, NotificationStatus.PARTIAL);
        var failed = notificationRepository.findByOrderIdAndStatus(orderId, NotificationStatus.FAILED);

        return ResponseEntity.ok(Map.of(
                "orderId", orderId,
                "sent", sent.isPresent(),
                "partial", partial.isPresent(),
                "failed", failed.isPresent(),
                "service", "notification-service"
        ));
    }

    @PostMapping("/test/email")
    public ResponseEntity<Map<String, String>> testEmail(@RequestBody EmailRequest request) {
        log.info("Test email request: {}", request);
        return ResponseEntity.ok(Map.of(
                "status", "OK",
                "message", "Email test endpoint - would send to: " + request.getTo()
        ));
    }

    @PostMapping("/test/sms")
    public ResponseEntity<Map<String, String>> testSms(@RequestBody SmsRequest request) {
        log.info("Test SMS request: {}", request);
        return ResponseEntity.ok(Map.of(
                "status", "OK",
                "message", "SMS test endpoint - would send to: " + request.getPhoneNumber()
        ));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        try {
            long count = notificationRepository.count();
            return ResponseEntity.ok(Map.of(
                    "status", "UP",
                    "notificationsCount", String.valueOf(count),
                    "service", "notification-service"
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
        long total = notificationRepository.count();
        long sent = notificationRepository.countByStatus(NotificationStatus.SENT);
        long partial = notificationRepository.countByStatus(NotificationStatus.PARTIAL);
        long failed = notificationRepository.countByStatus(NotificationStatus.FAILED);

        return ResponseEntity.ok(Map.of(
                "totalNotifications", total,
                "sent", sent,
                "partial", partial,
                "failed", failed,
                "service", "notification-service"
        ));
    }

    @PostMapping("/resend/{orderId}")
    public ResponseEntity<Map<String, String>> resendNotification(@PathVariable String orderId) {
        log.info("Manually resending notification for order: {}", orderId);
        // This would trigger a manual resend
        return ResponseEntity.ok(Map.of(
                "status", "QUEUED",
                "orderId", orderId,
                "message", "Notification resend queued"
        ));
    }

    @DeleteMapping("/cache")
    public ResponseEntity<Map<String, String>> clearCache() {
        notificationService.cleanupCache();
        return ResponseEntity.ok(Map.of(
                "status", "OK",
                "message", "Cache cleared"
        ));
    }
}