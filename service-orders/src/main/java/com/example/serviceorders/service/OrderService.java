package com.example.serviceorders.service;

import com.example.dto.CreateOrderDto;
import com.example.dto.OrderEvent;
import com.example.dto.OrderResponseDto;
import com.example.dto.OrderStatus;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderProducer orderProducer;

    public OrderResponseDto createOrder(CreateOrderDto createOrderDto) {
        OrderEvent orderEvent = OrderEvent.builder()
                .orderId(UUID.randomUUID()) // todo можно поиграться с различными вариантами БД пока эмитируем через Random
                .userId(createOrderDto.userId())
                .amount(createOrderDto.amount())
                .product(createOrderDto.product())
                .status(OrderStatus.NEW)
                .build();
        orderProducer.send(orderEvent);
        return OrderResponseDto.fromOrderEvent(orderEvent);
    }
}
