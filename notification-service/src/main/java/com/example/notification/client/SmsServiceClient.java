package com.example.notification.client;

import com.example.notification.dto.SmsRequest;
import com.example.notification.dto.SmsResponse;
import com.example.notification.exception.ServiceUnavailableException;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import static com.example.notification.config.CircuitBreakerConfig.SMS_SERVICE_CB;

@Slf4j
@Component
@RequiredArgsConstructor
public class SmsServiceClient {

    private final RestTemplate restTemplate;

    @Value("${services.sms.url:https://api.sms-provider.com}")
    private String smsServiceUrl;

    @CircuitBreaker(name = SMS_SERVICE_CB)
    @Retry(name = "smsRetry")
    public SmsResponse sendSms(SmsRequest request) {
        log.info("Sending SMS to: {}", request.getPhoneNumber());
        try {
            String url = smsServiceUrl + "/send";
            SmsResponse response = restTemplate.postForObject(url, request, SmsResponse.class);
            log.info("SMS sent to {}: {}", request.getPhoneNumber(),
                    response != null ? response.isSuccess() : false);
            return response;
        } catch (Exception e) {
            log.error("Error sending SMS to {}: {}", request.getPhoneNumber(), e.getMessage());
            throw new ServiceUnavailableException("SMS service unavailable", e);
        }
    }
}