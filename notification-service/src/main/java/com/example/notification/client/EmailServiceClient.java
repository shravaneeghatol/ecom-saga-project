package com.example.notification.client;

import com.example.notification.dto.EmailRequest;
import com.example.notification.dto.EmailResponse;
import com.example.notification.exception.ServiceUnavailableException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import static com.example.notification.config.CircuitBreakerConfig.EMAIL_SERVICE_CB;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmailServiceClient {

    private final RestTemplate restTemplate;

    @Value("${services.email.url:https://api.email-provider.com}")
    private String emailServiceUrl;

    @CircuitBreaker(name = EMAIL_SERVICE_CB)
    @Retry(name = "emailRetry")
    public EmailResponse sendEmail(EmailRequest request) {
        log.info("Sending email to: {}", request.getTo());
        try {
            String url = emailServiceUrl + "/send";
            EmailResponse response = restTemplate.postForObject(url, request, EmailResponse.class);
            log.info("Email sent to {}: {}", request.getTo(),
                    response != null ? response.isSuccess() : false);
            return response;
        } catch (Exception e) {
            log.error("Error sending email to {}: {}", request.getTo(), e.getMessage());
            throw new ServiceUnavailableException("Email service unavailable", e);
        }
    }
}