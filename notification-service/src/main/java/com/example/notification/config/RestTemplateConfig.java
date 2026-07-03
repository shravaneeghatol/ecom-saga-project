package com.example.notification.config;

import com.example.notification.exception.ServiceUnavailableException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;

@Configuration
public class RestTemplateConfig {

    @Bean
    public RestTemplate restTemplate() {
        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setErrorHandler(new DefaultResponseErrorHandler() {
            @Override
            public void handleError(ClientHttpResponse response) throws IOException {
                int statusCode = response.getRawStatusCode();
                if (statusCode >= 500) {
                    throw new ServiceUnavailableException("Service returned error " + statusCode);
                }
                super.handleError(response);
            }
        });
        return restTemplate;
    }
}
