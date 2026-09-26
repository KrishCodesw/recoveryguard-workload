package com.recoveryguard.events;

import java.util.List;

/**
 * Emitted by Inventory Service after consuming PaymentAuthorized. Topic: {@code inventory}.
 *
 * This is the event RecoveryGuard's flagship scenario targets: an order with
 * a durable OrderCreated + PaymentAuthorized but no InventoryReserved after
 * recovery is the canonical data-integrity failure for this workload.
 */
public record InventoryReservedEvent(
        EventEnvelope envelope,
        String reservationId,
        List<ReservedLine> reservedLines
) {
    public record ReservedLine(String sku, int quantity) {
    }
}
