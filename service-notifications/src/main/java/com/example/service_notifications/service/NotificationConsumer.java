package com.example.service_notifications.service;

import com.example.dto.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class NotificationConsumer {
    @KafkaListener(
            topics = "sent_orders",
            concurrency = "3"
    )
    public void consumeShippedOrder (
            OrderEvent orderEvent,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("NOTIFICATION: Order {} has been SHIPPED!", orderEvent.orderId());
        log.info("Received from partition: {}, offset: {}", partition, offset);
    }
}
