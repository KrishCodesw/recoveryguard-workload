package com.recoveryguard.order.web;

import com.recoveryguard.order.service.OrderPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderPublisher orderPublisher;

    public OrderController(OrderPublisher orderPublisher) {
        this.orderPublisher = orderPublisher;
    }

    @PostMapping
    public ResponseEntity<?> createOrder(@RequestBody CreateOrderRequest request) {
        return orderPublisher.publishOrderCreated(request)
                .thenApply(result -> ResponseEntity.status(HttpStatus.ACCEPTED)
                        .body(Map.of(
                                "partition", result.getRecordMetadata().partition(),
                                "offset", result.getRecordMetadata().offset()
                        )))
                .join();
    }
}
