package com.recoveryguard.events;

import java.math.BigDecimal;
import java.util.List;

/**
 * Emitted by Order Service. Topic: {@code orders}.
 */
public record OrderCreatedEvent(
        EventEnvelope envelope,
        String customerId,
        List<OrderLine> lines,
        BigDecimal totalAmount
) {
}
