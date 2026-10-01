package com.recoveryguard.order.service;

import com.recoveryguard.events.DurablePublisher;
import com.recoveryguard.events.EventEnvelope;
import com.recoveryguard.events.OrderCreatedEvent;
import com.recoveryguard.events.OrderLine;
import com.recoveryguard.events.ProduceOutcome;
import com.recoveryguard.order.web.CreateOrderRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Creates orders and publishes {@code OrderCreated}.
 *
 * <p>The returned {@link ProduceOutcome} resolves only once the broker has answered, and it is
 * resolved to {@code durable=false} rather than throwing when the write was not acknowledged. That
 * distinction is load-bearing: an order the broker never acknowledged must never enter the expected
 * state RecoveryGuard validates against, or the validator reports a missing event that was never
 * supposed to exist.
 */
@Service
public class OrderPublisher {

    /** Money is normalized to 2 decimal places so serialized amounts are byte-stable across runs. */
    public static final int MONEY_SCALE = 2;

    private final DurablePublisher publisher;
    private final String ordersTopic;

    public OrderPublisher(DurablePublisher publisher,
                          @Value("${recoveryguard.topics.orders}") String ordersTopic) {
        this.publisher = publisher;
        this.ordersTopic = ordersTopic;
    }

    public CompletableFuture<ProduceOutcome> publishOrderCreated(CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();

        List<OrderLine> lines = request.lines().stream()
                .map(l -> new OrderLine(l.sku(), l.quantity(), scale(l.unitPrice())))
                .toList();

        BigDecimal total = scale(lines.stream()
                .map(l -> l.unitPrice().multiply(BigDecimal.valueOf(l.quantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add));

        OrderCreatedEvent event = new OrderCreatedEvent(
                EventEnvelope.of(orderId, "OrderCreated"),
                request.customerId(),
                lines,
                total
        );

        // Keyed by orderId so every event for this order lands on the same partition -- required for
        // the per-order evidence trail RecoveryGuard reconstructs later.
        return publisher.publish(ordersTopic, orderId, event, event.envelope(), "OrderCreated");
    }

    /** Exposed for tests: the money normalization policy. */
    public static BigDecimal scale(BigDecimal value) {
        return value.setScale(MONEY_SCALE, RoundingMode.HALF_UP);
    }
}
