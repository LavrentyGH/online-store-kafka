package com.example.dto;

import java.math.BigDecimal;
import java.util.UUID;

public record OrderResponseDto(
        UUID orderId,
        UUID userId,
        BigDecimal amount,
        String product,
        OrderStatus status
) {
    public static OrderResponseDto fromOrderEvent (OrderEvent orderEvent) {
        return new OrderResponseDto(
                orderEvent.orderId(),
                orderEvent.userId(),
                orderEvent.amount(),
                orderEvent.product(),
                orderEvent.status()
        );
    }
}
