package com.example.inventory.client;

import com.example.inventory.dto.PaymentRequest;
import com.example.inventory.dto.PaymentResponse;
import com.example.inventory.exception.ServiceUnavailableException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import static com.example.inventory.config.CircuitBreakerConfig.PAYMENT_SERVICE_CB;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentServiceClient {

    private final RestTemplate restTemplate;

    @Value("${services.payment.url:http://payment-service:8082}")
    private String paymentServiceUrl;

    @CircuitBreaker(name = PAYMENT_SERVICE_CB)
    @Retry(name = "paymentRetry")
    public PaymentResponse checkPaymentStatus(String orderId) {
        log.info("Checking payment status for order: {}", orderId);
        try {
            String url = paymentServiceUrl + "/api/payment/status/" + orderId;
            PaymentResponse response = restTemplate.getForObject(url, PaymentResponse.class);
            log.info("Payment status for order {}: {}", orderId, response != null ? response.isSuccess() : "unknown");
            return response;
        } catch (Exception e) {
            log.error("Error checking payment status for order {}: {}", orderId, e.getMessage());
            throw new ServiceUnavailableException("Payment service unavailable", e);
        }
    }

    @CircuitBreaker(name = PAYMENT_SERVICE_CB)
    @Retry(name = "paymentRetry")
    public PaymentResponse processPayment(PaymentRequest request) {
        log.info("Processing payment for order: {}", request.getOrderId());
        try {
            String url = paymentServiceUrl + "/api/payment/process";
            PaymentResponse response = restTemplate.postForObject(url, request, PaymentResponse.class);
            log.info("Payment processed for order {}: {}", request.getOrderId(),
                    response != null ? response.isSuccess() : false);
            return response;
        } catch (Exception e) {
            log.error("Error processing payment for order {}: {}", request.getOrderId(), e.getMessage());
            throw new ServiceUnavailableException("Payment service unavailable", e);
        }
    }
}