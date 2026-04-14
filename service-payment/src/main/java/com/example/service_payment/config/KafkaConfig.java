package com.example.service_payment.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaConfig {

    @Bean
    public NewTopic paymentTopic() {
        return new NewTopic("payment_orders", 3, (short) 1);
    }
}
