package com.example.dto;


import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.util.UUID;

record OrderEvent(
        @JsonProperty("orderId")
        @NotNull
        UUID orderId,

        @JsonProperty("userId")
        @NotNull
        UUID userId,

        @JsonProperty("amount")
        @NotNull
        BigDecimal amount,

        @JsonProperty("product")
        @NotNull
        String product,

        @JsonProperty("stasus")
        @NotNull
        OrderStatus status
) {
    public OrderEvent {
        if (orderId == null) {
            orderId = UUID.randomUUID();
        }
        if (status == null) {
            status = OrderStatus.NEW;
        }
        if (amount != null && amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Amount must be positive: " + amount);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private UUID orderId;
        private UUID userId;
        private BigDecimal amount;
        private String product;
        private OrderStatus status;

        public Builder orderId(UUID orderId) {
            this.orderId = orderId;
            return this;
        }
        public Builder userId(UUID userId) {
            this.userId = userId;
            return this;
        }
        public Builder amount(BigDecimal amount) {
            this.amount = amount;
            return this;
        }
        public Builder product(String product) {
            this.product = product;
            return this;
        }
        public Builder status(OrderStatus status) {
            this.status = status;
            return this;
        }
        public OrderEvent build() {
            return new OrderEvent(orderId, userId, amount, product, status);

        }
    }
}