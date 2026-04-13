package com.example.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.UUID;

public record CreateOrderDto(
        @NotNull(message = "User ID is required")
        UUID userId,
        @Positive(message = "Amount must be positive")
        BigDecimal amount,
        @NotNull(message = "Product name is required")
        String product
) {

}
