package com.example.service_shipping.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
@Slf4j
public class KafkaConfig {


    @Value("${shipping.retry.max-attempts:3}")
    private int maxAttempts;

    @Value("${shipping.retry.backoff-delay-ms:3000}")
    private long backoffDelay;

    @Value("${shipping.retry.multiplier:2.0}")
    private double multiplier;

    @Bean
    public NewTopic shippingTopic() {
        return new NewTopic("sent_orders", 3, (short) 3);
    }

    @Bean
    public NewTopic shippingDltTopic() {
        return new NewTopic("shipping_dlt", 3, (short) 3);
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    log.error("Moving to DLT after {} attempts. Order: {}",
                            maxAttempts, record.key());
                    return new org.apache.kafka.common.TopicPartition("shipping_dlt", record.partition());
                }
        );

        ExponentialBackOff backOff = new ExponentialBackOff(backoffDelay, multiplier);
        backOff.setMaxElapsedTime(60000); // Максимум 60 секунд

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("🔄 Shipping retry {} for order: {}", deliveryAttempt, record.key())
        );

        errorHandler.addNotRetryableExceptions(
                IllegalArgumentException.class,
                NullPointerException.class
        );

        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory,
            DefaultErrorHandler errorHandler) {

        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
}
