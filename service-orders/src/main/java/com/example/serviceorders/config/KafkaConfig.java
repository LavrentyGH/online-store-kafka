package com.example.serviceorders.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {
    @Value("${kafka.topics.orders.new}")
    private String newOrdersTopic;

    @Value("${kafka.topics.orders.partitions:3}")
    private int partitions;

    @Value("${kafka.topics.orders.replicas:1}")
    private short replicas;

    @Bean
    public NewTopic ordersTopic() {
        return new NewTopic(newOrdersTopic, partitions, replicas);
    }
}
