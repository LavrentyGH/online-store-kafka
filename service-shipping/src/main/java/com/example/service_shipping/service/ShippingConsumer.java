package com.example.service_shipping.service;

import com.example.dto.OrderEvent;
import com.example.dto.OrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class ShippingConsumer {
    private final ShippingProducer shippingProducer;
    private final Random random = new Random();

    private final ConcurrentHashMap<String, Boolean> processedOrders = new ConcurrentHashMap<>();

    @Value("${payment.processing-delay-ms:2000}")
    private long processingDelayMs;

    @Value("${payment.success-rate:0.98}")
    private double successRate;

    @KafkaListener(
            topics = "payed_orders",
            concurrency = "3"
    )
    public void processNewOrder(OrderEvent orderEvent,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                @Header(KafkaHeaders.OFFSET) long offset) {
        String orderId = orderEvent.orderId().toString();
        if (processedOrders.containsKey(orderId)) {
            log.warn("Order {} already processed in shipping!", orderId);

            if (!processedOrders.get(orderId)) {
                log.info("Resending to sent_orders...");
                shippingProducer.sendShippingOrder(orderEvent.withStatus(OrderStatus.SENT));
                processedOrders.put(orderId, true);
            }
            return;
        }

        processedOrders.put(orderId, false);

        try {
            TimeUnit.MILLISECONDS.sleep(processingDelayMs);

            boolean shippingSuccess  = random.nextDouble() < successRate;

            if (shippingSuccess) {
                OrderEvent sentOrder = orderEvent.withStatus(OrderStatus.SENT);

                log.info(" Shipping successful for order: {}. Sending to payed_orders",
                        orderEvent.orderId());

                shippingProducer.sendShippingOrder(sentOrder);
            } else {
                log.warn(" Shipping FAILED for order: {}. Sending to DLQ (not implemented)",
                        orderEvent.orderId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Payment processing interrupted for order: {}", orderEvent.orderId());
            throw new RuntimeException(e);
        }
    }
}
