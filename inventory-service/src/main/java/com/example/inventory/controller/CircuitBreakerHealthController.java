package com.example.inventory.controller;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/circuit-breaker")
@RequiredArgsConstructor
public class CircuitBreakerHealthController {

    private final CircuitBreakerRegistry circuitBreakerRegistry;

    @GetMapping("/status")
    public Map<String, Object> getCircuitBreakerStatus() {
        Map<String, Object> status = new HashMap<>();

        circuitBreakerRegistry.getAllCircuitBreakers().forEach(cb -> {
            Map<String, Object> cbStatus = new HashMap<>();
            cbStatus.put("state", cb.getState().toString());

            // Get metrics
            CircuitBreaker.Metrics metrics = cb.getMetrics();
            cbStatus.put("failureRate", metrics.getFailureRate());
            cbStatus.put("numberOfFailedCalls", metrics.getNumberOfFailedCalls());
            cbStatus.put("numberOfSuccessfulCalls", metrics.getNumberOfSuccessfulCalls());
            cbStatus.put("numberOfCalls", metrics.getNumberOfFailedCalls() + metrics.getNumberOfSuccessfulCalls());

            // Additional metrics
            cbStatus.put("numberOfBufferedCalls", metrics.getNumberOfBufferedCalls());
            cbStatus.put("numberOfSlowCalls", metrics.getNumberOfSlowCalls());
            cbStatus.put("numberOfSlowFailedCalls", metrics.getNumberOfSlowFailedCalls());

            status.put(cb.getName(), cbStatus);
        });

        return status;
    }

    @GetMapping("/reset")
    public String resetCircuitBreakers() {
        circuitBreakerRegistry.getAllCircuitBreakers().forEach(cb -> {
            cb.reset();
            log.info("Circuit breaker {} reset", cb.getName());
        });
        return "All circuit breakers reset";
    }

    @GetMapping("/health")
    public Map<String, Boolean> getHealthStatus() {
        Map<String, Boolean> health = new HashMap<>();

        circuitBreakerRegistry.getAllCircuitBreakers().forEach(cb -> {
            health.put(cb.getName(), cb.getState() != CircuitBreaker.State.OPEN);
        });

        return health;
    }
}