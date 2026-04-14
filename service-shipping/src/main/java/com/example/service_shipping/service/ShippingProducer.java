package com.example.service_shipping.service;

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
public class ShippingProducer {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private static final String TOPIC = "sent_orders";
    public void sendShippingOrder(OrderEvent sentOrder) {
        String key = sentOrder.orderId().toString();

        log.info("Sending paid order to Kafka: {}", sentOrder.orderId().toString());

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(TOPIC, key, sentOrder);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Shipped order sent successfully: {}, partition: {}, offset: {}",
                        sentOrder.orderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error(" Failed to send shipped order: {}", sentOrder.orderId(), ex);
                //todo Обеспечьте механизм обработки ошибок и переотправки сообщений в случае сбоев.
            }
        });
    }
}
