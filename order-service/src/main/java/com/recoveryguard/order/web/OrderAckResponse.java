package com.recoveryguard.order.web;

import com.recoveryguard.events.ProduceOutcome;

import java.time.Instant;
import java.util.UUID;

/**
 * Response body for {@code POST /orders}.
 *
 * <p><b>Why {@code orderId} is here.</b> It previously was not: the controller returned only
 * {@code partition} and {@code offset}, while {@code orderId} was generated server-side inside
 * {@code OrderPublisher} and never surfaced. An external workload driver therefore could not record
 * which orders it had created without scraping Kafka to rediscover its own requests -- which defeats
 * PRD FR-10 (record workload parameters) and the reproducibility requirement, and makes the
 * independent expected-state ledger impossible to reconcile.
 *
 * <p>{@code durable} tells the caller whether the broker actually acknowledged the write. A
 * {@code durable=false} response is an explicit negative acknowledgement, not an error to be retried
 * blindly: the caller records that the order was never acknowledged and excludes it from expected
 * state.
 */
public record OrderAckResponse(
        String orderId,
        UUID eventId,
        String eventType,
        String topic,
        Integer partition,
        Long offset,
        Instant ackedAt,
        boolean durable,
        String reason
) {
    public static OrderAckResponse from(ProduceOutcome outcome) {
        return new OrderAckResponse(
                outcome.orderId(),
                outcome.eventId(),
                outcome.eventType(),
                outcome.topic(),
                outcome.partition(),
                outcome.offset(),
                outcome.at(),
                outcome.durable(),
                outcome.reason()
        );
    }
}
