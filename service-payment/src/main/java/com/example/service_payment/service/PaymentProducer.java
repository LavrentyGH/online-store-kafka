package com.example.service_payment.service;

import com.example.dto.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentProducer {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private static final String TOPIC = "payed_orders";

    public void sendPayedOrder(OrderEvent paidOrder) {
        String key = paidOrder.orderId().toString();

        log.info("Sending paid order to Kafka: {}", paidOrder.orderId().toString());

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(TOPIC, key, paidOrder);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Paid order sent successfully: {}, partition: {}, offset: {}",
                        paidOrder.orderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error(" Failed to send paid order: {}", paidOrder.orderId(), ex);
            }
        });
    }
}
