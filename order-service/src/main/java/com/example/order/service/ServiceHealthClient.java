package com.example.order.service;

import com.example.order.config.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.time.Duration;
import java.util.Map;

/**
 * HTTP client that checks the health / reachability of each downstream service.
 *
 * WHY THIS CLASS EXISTS
 * ─────────────────────
 * Order-service is choreography-based: it does NOT call inventory/payment/notification
 * via HTTP in the normal saga flow.  Events travel via Kafka + the outbox.
 *
 * However you still need HTTP circuit breakers here for two reasons:
 *
 *  1. ADMIN / MANUAL RETRY  – If the operator wants to know whether a downstream
 *     service is alive before re-queueing FAILED outbox rows, this client can be
 *     used from OrderController (GET /api/orders/services/health).
 *
 *  2. PATTERN TEMPLATE  – Every other service (inventory, payment, notification)
 *     WILL call order-service or each other via HTTP for specific operations
 *     (e.g. payment-service calling inventory-service to confirm reservation).
 *     Copy this class into those services and swap the @CircuitBreaker name.
 *
 * HOW THE CIRCUIT BREAKER IS APPLIED
 * ────────────────────────────────────
 * Each method carries its own @CircuitBreaker pointing to a different instance
 * (inventoryService / paymentService / notificationService).  This means:
 *
 *  • inventory going down does NOT affect payment's or notification's breaker.
 *  • Each breaker's state is tracked and exposed on /actuator/circuitbreakers
 *    independently.
 *
 * FALLBACK METHODS
 * ─────────────────
 * Each public method has a private *Fallback companion.  When the breaker is OPEN
 * (or the real call fails), Resilience4j automatically calls the fallback.
 * The fallback returns a safe default so the caller never gets an exception —
 * it just sees "SERVICE DOWN" in the response and can decide what to do.
 */
@Service
@Slf4j
public class ServiceHealthClient {

    private final WebClient inventoryClient;
    private final WebClient paymentClient;
    private final WebClient notificationClient;

    public ServiceHealthClient(
            @Value("${services.inventory.url}") String inventoryUrl,
            @Value("${services.payment.url}")   String paymentUrl,
            @Value("${services.notification.url}") String notificationUrl
    ) {
        // Each service gets its own WebClient with a 3-second response timeout.
        // The circuit breaker's slow-call-duration-threshold is also 3s (application.yml),
        // so a call that hangs longer is counted as a slow call toward the failure rate.
        this.inventoryClient    = buildClient(inventoryUrl);
        this.paymentClient      = buildClient(paymentUrl);
        this.notificationClient = buildClient(notificationUrl);
    }

    // ── Inventory Service ──────────────────────────────────────────────────────

    /**
     * Calls GET /actuator/health on inventory-service.
     * Protected by the "inventoryService" circuit breaker.
     * Falls back to {@link #inventoryFallback} when OPEN or on error.
     */
    @CircuitBreaker(name = CircuitBreakerConfig.INVENTORY_SERVICE_BREAKER,
            fallbackMethod = "inventoryFallback")
    public Map<String, Object> checkInventoryHealth() {
        String status = inventoryClient.get()
                .uri("/actuator/health")
                .retrieve()
                .bodyToMono(String.class)
                .timeout(Duration.ofSeconds(3))
                .block();
        log.info("[CB:inventoryService] health check OK: {}", status);
        return Map.of("service", "inventory", "status", "UP", "response", status);
    }

    @SuppressWarnings("unused")  // called by Resilience4j via reflection
    private Map<String, Object> inventoryFallback(Exception ex) {
        String reason = resolveReason("inventoryService", ex);
        log.warn("[CB:inventoryService] fallback triggered — {}", reason);
        return Map.of("service", "inventory", "status", "DOWN", "reason", reason);
    }

    // ── Payment Service ────────────────────────────────────────────────────────

    /**
     * Calls GET /actuator/health on payment-service.
     * Protected by the "paymentService" circuit breaker.
     */
    @CircuitBreaker(name = CircuitBreakerConfig.PAYMENT_SERVICE_BREAKER,
            fallbackMethod = "paymentFallback")
    public Map<String, Object> checkPaymentHealth() {
        String status = paymentClient.get()
                .uri("/actuator/health")
                .retrieve()
                .bodyToMono(String.class)
                .timeout(Duration.ofSeconds(3))
                .block();
        log.info("[CB:paymentService] health check OK: {}", status);
        return Map.of("service", "payment", "status", "UP", "response", status);
    }

    @SuppressWarnings("unused")
    private Map<String, Object> paymentFallback(Exception ex) {
        String reason = resolveReason("paymentService", ex);
        log.warn("[CB:paymentService] fallback triggered — {}", reason);
        return Map.of("service", "payment", "status", "DOWN", "reason", reason);
    }

    // ── Notification Service ───────────────────────────────────────────────────

    /**
     * Calls GET /actuator/health on notification-service.
     * Protected by the "notificationService" circuit breaker.
     */
    @CircuitBreaker(name = CircuitBreakerConfig.NOTIFICATION_SERVICE_BREAKER,
            fallbackMethod = "notificationFallback")
    public Map<String, Object> checkNotificationHealth() {
        String status = notificationClient.get()
                .uri("/actuator/health")
                .retrieve()
                .bodyToMono(String.class)
                .timeout(Duration.ofSeconds(3))
                .block();
        log.info("[CB:notificationService] health check OK: {}", status);
        return Map.of("service", "notification", "status", "UP", "response", status);
    }

    @SuppressWarnings("unused")
    private Map<String, Object> notificationFallback(Exception ex) {
        String reason = resolveReason("notificationService", ex);
        log.warn("[CB:notificationService] fallback triggered — {}", reason);
        return Map.of("service", "notification", "status", "DOWN", "reason", reason);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static WebClient buildClient(String baseUrl) {
        return WebClient.builder()
                .baseUrl(baseUrl)
                .build();
    }

    /**
     * Converts an exception to a human-readable reason string for the fallback response.
     * Distinguishes between:
     *  - CallNotPermittedException → breaker is OPEN (fast-fail, no network call made)
     *  - WebClientResponseException → service returned an HTTP error (4xx / 5xx)
     *  - anything else → network / timeout issue
     */
    private static String resolveReason(String breakerName, Exception ex) {
        if (ex instanceof CallNotPermittedException) {
            return "Circuit breaker [" + breakerName + "] is OPEN — calls blocked until recovery";
        }
        if (ex instanceof WebClientResponseException wcEx) {
            return "HTTP " + wcEx.getStatusCode() + " from " + breakerName;
        }
        return ex.getClass().getSimpleName() + ": " + ex.getMessage();
    }
}