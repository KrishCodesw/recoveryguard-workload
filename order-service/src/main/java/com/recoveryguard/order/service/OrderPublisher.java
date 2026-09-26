package com.recoveryguard.order.service;

import com.recoveryguard.events.EventEnvelope;
import com.recoveryguard.events.OrderCreatedEvent;
import com.recoveryguard.events.OrderLine;
import com.recoveryguard.order.web.CreateOrderRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class OrderPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String ordersTopic;

    public OrderPublisher(KafkaTemplate<String, Object> kafkaTemplate,
                           @Value("${recoveryguard.topics.orders}") String ordersTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.ordersTopic = ordersTopic;
    }

    public CompletableFuture<SendResult<String, Object>> publishOrderCreated(CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();

        List<OrderLine> lines = request.lines().stream()
                .map(l -> new OrderLine(l.sku(), l.quantity(), l.unitPrice()))
                .toList();

        BigDecimal total = lines.stream()
                .map(l -> l.unitPrice().multiply(BigDecimal.valueOf(l.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        OrderCreatedEvent event = new OrderCreatedEvent(
                EventEnvelope.of(orderId, "OrderCreated"),
                request.customerId(),
                lines,
                total
        );

        // Keyed by orderId so every event for this order lands on the same
        // partition -- required for the per-order evidence trail RecoveryGuard
        // reconstructs later.
        return kafkaTemplate.send(ordersTopic, orderId, event);
    }
}
