package com.example.serviceorders.controller;


import com.example.dto.CreateOrderDto;
import com.example.dto.OrderResponseDto;
import com.example.serviceorders.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("api/v1/orders")
public class OrderController {
    private final OrderService orderService;

    @PostMapping()
    public ResponseEntity<OrderResponseDto> createOrder(@Validated @RequestBody CreateOrderDto createOrderDto) {
        return ResponseEntity.status(HttpStatus.CREATED).body(orderService.createOrder(createOrderDto));
    }
}
