package com.recoveryguard.events.ack;

import java.time.Instant;
import java.util.UUID;

/**
 * Immutable ground-truth record written only after Kafka acknowledges a produced event.
 *
 * The record deliberately stores Kafka's returned topic/partition/offset rather than
 * attempting to infer them from application logs. RecoveryGuard can therefore use this
 * file as an independent acknowledgement trail and correlate it with Kafka evidence.
 */
public record AcknowledgementRecord(
        String experimentId,
        String producerService,
        UUID eventId,
        String orderId,
        String eventType,
        String topic,
        int partition,
        long offset,
        Instant producedAt,
        Instant acknowledgedAt
) {
}
