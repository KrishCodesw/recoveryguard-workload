package com.recoveryguard.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Common fields carried by every business event in the workload.
 *
 * These fields are what RecoveryGuard's evidence collector correlates
 * against Kafka offsets, replica/ISR history, and the ground-truth
 * acknowledgement ledger. Do not remove or rename them without updating
 * recoveryguard-core's evidence adapters.
 *
 * @param eventId   unique id for this event instance
 * @param orderId   the business correlation id (order this event belongs to)
 * @param eventType the event type name, e.g. "OrderCreated"
 * @param producedAt wall-clock time the producing service emitted the event
 */
public record EventEnvelope(
        UUID eventId,
        String orderId,
        String eventType,
        Instant producedAt
) {
    public static EventEnvelope of(String orderId, String eventType) {
        return new EventEnvelope(UUID.randomUUID(), orderId, eventType, Instant.now());
    }
}
