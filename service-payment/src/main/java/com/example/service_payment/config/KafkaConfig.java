package com.example.service_payment.config;

import com.example.dto.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
@Slf4j
public class KafkaConfig {

    @Value("${payment.retry.max-attempts:3}")
    private int maxAttempts;

    @Value("${payment.retry.backoff-delay-ms:2000}")
    private long backoffDelay;

    @Value("${payment.retry.multiplier:2.0}")
    private double multiplier;


    @Bean
    public NewTopic paymentTopic() {
        return new NewTopic(
                "payment_orders",
                3,
                (short) 3);
    }


    @Bean
    public NewTopic paymentDltTopic() {
        return new NewTopic("payment_dlt", 1, (short) 1);
    }

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {

        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    log.error("Moving to DLT after {} attempts. Topic: {}, Key: {}, Error: {}",
                            maxAttempts, record.topic(), record.key(), ex.getMessage());
                    return new org.apache.kafka.common.TopicPartition("payment_dlt", record.partition());
                }
        );

        ExponentialBackOff backOff = new ExponentialBackOff(backoffDelay, multiplier);
        backOff.setMaxElapsedTime(30000);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("Retry attempt {} for record: {}, error: {}",
                        deliveryAttempt, record.key(), ex.getMessage())
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
