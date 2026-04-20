package com.example.service_payment.service;

import com.example.dto.OrderEvent;
import com.example.dto.OrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentConsumer {
    private final PaymentProducer paymentProducer;
    private final Random random = new Random();
    private final ConcurrentHashMap<String, Boolean> processedOrders = new ConcurrentHashMap<>();

    @Value("${payment.processing-delay-ms:1000}")
    private long processingDelayMs;

    @Value("${payment.success-rate:0.95}")
    private double successRate;

    @KafkaListener(
            topics = "new_orders",
            concurrency = "3"
    )

    public void processNewOrder(OrderEvent orderEvent,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                @Header(KafkaHeaders.OFFSET) long offset) {
        String kay = orderEvent.orderId().toString();
        if(processedOrders.containsKey(kay)){
            if(!processedOrders.get(kay)){
                paymentProducer.sendPayedOrder(orderEvent.withStatus(OrderStatus.PAYED));
                processedOrders.put(kay, true);
            }
            return;
        }

        processedOrders.put(kay, false);

        try {
            TimeUnit.MILLISECONDS.sleep(processingDelayMs);

            boolean paymentSuccess = random.nextDouble() < successRate;

            if (paymentSuccess) {
                OrderEvent paidOrder = orderEvent.withStatus(OrderStatus.PAYED);

                log.info(" Payment successful for order: {}. Sending to payed_orders",
                        orderEvent.orderId());

                paymentProducer.sendPayedOrder(paidOrder);
                processedOrders.put(kay, true);
            } else {
                log.warn(" Payment FAILED for order: {}. Sending to DLQ (not implemented)",
                        orderEvent.orderId());
                processedOrders.remove(kay);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Payment processing interrupted for order: {}", orderEvent.orderId());
            throw new RuntimeException("Payment processing interrupted", e);
        }
    }
}
