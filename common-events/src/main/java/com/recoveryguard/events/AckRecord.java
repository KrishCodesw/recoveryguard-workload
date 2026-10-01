package com.recoveryguard.events;

import java.time.Instant;
import java.util.UUID;

/**
 * One row of the ground-truth acknowledgement ledger.
 *
 * <p>This is the record RecoveryGuard compares recovered Kafka state against. It must be produced
 * by the workload, at acknowledgement time, and stored somewhere that is <b>not Kafka</b> -- a
 * ledger that lives in the log being validated is erased by the same broker failure that erases the
 * event, which would make the central check circular: you cannot detect log loss using the log that
 * lost it.
 *
 * <p>{@code role} distinguishes what actually happened, which is what lets the validation engine
 * separate a genuine integrity failure from a workload-side non-acknowledgement:
 * <ul>
 *   <li>{@link Role#PRODUCED} -- the broker acknowledged the write; this event is <em>expected
 *       state</em> and must survive recovery.</li>
 *   <li>{@link Role#REJECTED} -- the produce never reached a durable ack (timeout, buffer full,
 *       {@code NOT_ENOUGH_REPLICAS}). The application was told so. This event must <b>not</b> be
 *       counted as missing after recovery, because it was never acknowledged.</li>
 *   <li>{@link Role#CONSUMED} -- a listener received the record; used to reconstruct the causal
 *       chain and consumer progress.</li>
 *   <li>{@link Role#DROPPED} -- the workload itself gave up on a record and routed it to a dead
 *       letter topic. Attributable loss, not a recovery defect.</li>
 * </ul>
 *
 * @param schemaVersion ledger schema version
 * @param experimentId  identifier of the experiment run this record belongs to
 * @param service       producing service, e.g. "payment-service"
 * @param role          what happened, see above
 * @param eventId       the event this record concerns
 * @param orderId       business correlation id
 * @param eventType     e.g. "PaymentAuthorized"
 * @param topic         target topic, or {@code null} if the produce never got that far
 * @param partition     partition the broker placed the record in, or {@code null}
 * @param offset        offset assigned by the broker, or {@code null}
 * @param causationId   id of the causing event, or {@code null} at a chain root
 * @param at            wall-clock time of the acknowledgement or rejection
 * @param isrSize       in-service replica count observed at ack time, if known
 * @param reason        machine-readable failure reason for REJECTED/DROPPED, else {@code null}
 */
public record AckRecord(
        int schemaVersion,
        String experimentId,
        String service,
        Role role,
        UUID eventId,
        String orderId,
        String eventType,
        String topic,
        Integer partition,
        Long offset,
        UUID causationId,
        Instant at,
        Integer isrSize,
        String reason
) {
    /** Ledger schema version. Bump on breaking changes. */
    public static final int SCHEMA_VERSION = 1;

    /** Sentinel used when no experiment id was supplied. */
    public static final String UNSCOPED_EXPERIMENT = "unscoped";

    public enum Role {
        /** Broker acknowledged the write -- this is expected state. */
        PRODUCED,
        /** The produce never became durable and the caller was informed. */
        REJECTED,
        /** A listener received and acted on the record. */
        CONSUMED,
        /** The workload abandoned the record to a dead letter topic. */
        DROPPED
    }

    public boolean isDurable() {
        return role == Role.PRODUCED;
    }
}
