package com.recoveryguard.events;

import java.time.Instant;
import java.util.UUID;

/**
 * The result of attempting to publish one event, resolved only after the broker has answered.
 *
 * <p>The {@code durable} flag is the whole point. Previously the workload had two ways to lose an
 * event and no way to tell them apart: a produce that timed out looked identical, from outside, to a
 * produce that was never attempted. {@code durable=false} with a {@code reason} makes
 * "never acknowledged" an explicit, recordable outcome -- and an order that was never acknowledged
 * must never enter the expected state RecoveryGuard validates against, or the validator reports a
 * missing event that was never supposed to exist.
 *
 * @param eventId   the event that was (or was not) published
 * @param orderId   business correlation id
 * @param eventType e.g. "OrderCreated"
 * @param topic     target topic
 * @param partition partition assigned by the broker, or {@code null} if not durable
 * @param offset    offset assigned by the broker, or {@code null} if not durable
 * @param at        when the outcome was resolved
 * @param durable   {@code true} only if the broker acknowledged the write under the contract
 * @param reason    machine-readable reason when {@code durable} is {@code false}
 */
public record ProduceOutcome(
        UUID eventId,
        String orderId,
        String eventType,
        String topic,
        Integer partition,
        Long offset,
        Instant at,
        boolean durable,
        String reason
) {
    public static ProduceOutcome durable(EventEnvelope e, String eventType, String topic,
                                         int partition, long offset) {
        return new ProduceOutcome(e.eventId(), e.orderId(), eventType, topic, partition, offset,
                Instant.now(), true, null);
    }

    public static ProduceOutcome rejected(EventEnvelope e, String eventType, String topic, String reason) {
        return new ProduceOutcome(e.eventId(), e.orderId(), eventType, topic, null, null,
                Instant.now(), false, reason);
    }
}
