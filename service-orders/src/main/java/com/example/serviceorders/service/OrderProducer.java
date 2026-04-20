package com.example.serviceorders.service;

import com.example.dto.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderProducer {
    @Value("${kafka.topics.orders.new}")
    private String topic;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    public void send(OrderEvent orderEvent) {
        String key = orderEvent.orderId().toString();

        log.info("Sending message to topic {}", topic);

        CompletableFuture<SendResult<String, OrderEvent>> future = kafkaTemplate.send(topic, key, orderEvent);
        try {
            SendResult<String, OrderEvent> result = future.get(5, TimeUnit.SECONDS);

            log.info("sent order: {}, partition: {}, offset: {}",
                    orderEvent.orderId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());

        } catch (Exception e) {
            log.error("Failed send order: {}", orderEvent.orderId());
            throw new RuntimeException("Failed to send " + e);
        }
    }
}
