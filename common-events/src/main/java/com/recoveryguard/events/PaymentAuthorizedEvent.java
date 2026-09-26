package com.recoveryguard.events;

import java.math.BigDecimal;
import java.util.List;

/**
 * Emitted by Payment Service after consuming OrderCreated. Topic: {@code payments}.
 *
 * Carries the order lines forward so Inventory Service can reserve stock
 * without joining back against OrderCreated -- keeps each consumer a simple
 * single-topic listener, which matters for how RecoveryGuard reconstructs
 * per-order evidence later.
 */
public record PaymentAuthorizedEvent(
        EventEnvelope envelope,
        String paymentId,
        BigDecimal authorizedAmount,
        List<OrderLine> lines
) {
}
