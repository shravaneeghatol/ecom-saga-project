package com.example.order.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the topic this service OWNS (publishes to).
 * Spring's KafkaAdmin auto-creates this on application startup against the broker.
 * Retry / DLT topics for events this service CONSUMES are created automatically
 * by the @RetryableTopic annotation on the listener methods (see SagaEventListener).
 */
@Configuration
public class KafkaTopicConfig {

    public static final String ORDER_EVENTS_TOPIC = "order.events";
    public static final String ORDER_EVENTS_FAILURE_TOPIC = "order.events-failure";

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(ORDER_EVENTS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderFailureTopic() {
        return TopicBuilder.name("order.events-failure")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
