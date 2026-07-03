package com.example.inventory.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String INVENTORY_EVENTS_TOPIC = "inventory.events";
    public static final String INVENTORY_EVENTS_FAILURE_TOPIC = "inventory.events-failure";

    @Bean
    public NewTopic inventoryEventsTopic() {
        return TopicBuilder.name(INVENTORY_EVENTS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic inventoryFailureTopic() {
        return TopicBuilder.name("inventory.events-failure")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
