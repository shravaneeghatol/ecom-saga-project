package com.example.payment.client;

import com.example.payment.dto.OrderCompensationRequest;
import com.example.payment.dto.OrderCompensationResponse;
import com.example.payment.exception.ServiceUnavailableException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import static com.example.payment.config.CircuitBreakerConfig.ORDER_SERVICE_CB;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderServiceClient {

    private final RestTemplate restTemplate;

    @Value("${services.order.url:http://order-service:8081}")
    private String orderServiceUrl;

    @CircuitBreaker(name = ORDER_SERVICE_CB)
    @Retry(name = "orderRetry")
    public OrderCompensationResponse compensateOrder(String orderId, String reason) {
        log.info("Compensating order: {} due to: {}", orderId, reason);
        try {
            String url = orderServiceUrl + "/api/order/compensate";
            OrderCompensationRequest request = OrderCompensationRequest.builder()
                    .orderId(orderId)
                    .reason(reason)
                    .build();
            OrderCompensationResponse response = restTemplate.postForObject(url, request, OrderCompensationResponse.class);
            log.info("Order compensated: {}", orderId);
            return response;
        } catch (Exception e) {
            log.error("Error compensating order {}: {}", orderId, e.getMessage());
            throw new ServiceUnavailableException("Order service unavailable", e);
        }
    }
}