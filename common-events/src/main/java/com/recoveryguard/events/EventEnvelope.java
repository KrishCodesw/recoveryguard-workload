package com.recoveryguard.events;

import java.time.Instant;
import java.util.UUID;

/**
 * Common fields carried by every business event in the workload.
 *
 * <p>These fields are what RecoveryGuard's evidence collector correlates against Kafka offsets,
 * replica/ISR history, and the ground-truth acknowledgement ledger written by
 * {@link AcknowledgementLedger}. Do not remove or rename them without updating recoveryguard-core's
 * evidence adapters.
 *
 * <p><b>Why causationId exists.</b> The workload is at-least-once by design: a child event can be
 * produced and broker-acknowledged, and then the consumer's offset commit can fail (typically
 * because a broker died at exactly that moment). On restart the parent record is redelivered and a
 * second child event appears. Without {@code causationId} the validation engine cannot distinguish
 * that benign redelivery from genuine replica divergence, and the two must produce different
 * verdicts. With it, a duplicate is identifiable as a duplicate.
 *
 * @param schemaVersion event-schema version; bump on any breaking change so consumers and the
 *                      evidence collector can reject or adapt instead of silently misparsing
 * @param eventId       unique id for this event instance
 * @param orderId       the business correlation id (order this event belongs to)
 * @param eventType     the event type name, e.g. "OrderCreated"
 * @param causationId   eventId of the event that directly caused this one; {@code null} for the
 *                      root of a causal chain
 * @param producedAt    wall-clock time the producing service emitted the event
 */
public record EventEnvelope(
        int schemaVersion,
        UUID eventId,
        String orderId,
        String eventType,
        UUID causationId,
        Instant producedAt
) {
    /** Current wire schema version. */
    public static final int SCHEMA_VERSION = 1;

    /**
     * Creates an envelope for the root of a causal chain (no causing event).
     */
    public static EventEnvelope of(String orderId, String eventType) {
        return new EventEnvelope(SCHEMA_VERSION, UUID.randomUUID(), orderId, eventType, null, Instant.now());
    }

    /**
     * Creates an envelope for an event caused by {@code parent}, inheriting the parent's
     * {@code eventId} as this event's {@code causationId} and the same {@code orderId}.
     */
    public static EventEnvelope childOf(EventEnvelope parent, String eventType) {
        return new EventEnvelope(SCHEMA_VERSION, UUID.randomUUID(), parent.orderId(), eventType,
                parent.eventId(), Instant.now());
    }
}
