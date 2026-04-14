package com.example.service_shipping.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic shippingTopic() {
        return new NewTopic("sent_orders", 3, (short) 1);
    }
}
