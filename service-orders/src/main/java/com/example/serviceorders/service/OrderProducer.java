package com.example.serviceorders.service;

import com.example.dto.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderProducer {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private static  final String TOPIC = "new_orders";
    public void send(OrderEvent orderEvent) {
        String key = orderEvent.orderId().toString();

        log.info("Sending message to topic {}", TOPIC);

        CompletableFuture<SendResult<String, OrderEvent>> future = kafkaTemplate.send(TOPIC, key, orderEvent);
        future.whenComplete((result, exception) -> {
            if (exception == null) {
                log.info("sent order: {}, partition: {}, offset: {}",
                orderEvent.orderId(),
                result.getRecordMetadata().partition(),
                result.getRecordMetadata().offset());
            } else {
                log.error("Failed send order: {}", orderEvent.orderId());
            }
        });
    }
}
