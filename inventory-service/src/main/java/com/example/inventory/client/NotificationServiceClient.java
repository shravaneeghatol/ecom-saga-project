package com.example.inventory.client;

import com.example.inventory.dto.NotificationRequest;
import com.example.inventory.dto.NotificationResponse;
import com.example.inventory.exception.ServiceUnavailableException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import static com.example.inventory.config.CircuitBreakerConfig.NOTIFICATION_SERVICE_CB;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationServiceClient {

    private final RestTemplate restTemplate;

    @Value("${services.notification.url:http://notification-service:8083}")
    private String notificationServiceUrl;

    @CircuitBreaker(name = NOTIFICATION_SERVICE_CB)
    @Retry(name = "notificationRetry")
    public NotificationResponse sendNotification(NotificationRequest request) {
        log.info("Sending notification for order: {}", request.getOrderId());
        try {
            String url = notificationServiceUrl + "/api/notification/send";
            NotificationResponse response = restTemplate.postForObject(url, request, NotificationResponse.class);
            log.info("Notification sent for order {}: {}", request.getOrderId(),
                    response != null ? response.isSuccess() : false);
            return response;
        } catch (Exception e) {
            log.error("Error sending notification for order {}: {}", request.getOrderId(), e.getMessage());
            throw new ServiceUnavailableException("Notification service unavailable", e);
        }
    }
}