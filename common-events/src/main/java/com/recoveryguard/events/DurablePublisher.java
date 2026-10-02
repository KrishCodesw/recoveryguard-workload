package com.recoveryguard.events;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes an event and resolves only once the broker has answered, recording the outcome in the
 * acknowledgement ledger either way.
 *
 * <p><b>The bug this replaces.</b> The chain listeners previously called
 * {@code kafkaTemplate.send(topic, key, event)} and discarded the returned future, while their
 * containers ran in {@code AckMode.RECORD} -- so the consumer offset was committed the instant the
 * {@code void} listener method returned, <em>before any broker had acknowledged the child event</em>.
 * If that produce then failed during an injected outage, the parent offset was already committed,
 * the record was never redelivered, and the child event simply ceased to exist.
 *
 * <p>That produces precisely the signature RecoveryGuard's flagship scenario hunts for -- "payment
 * authorized but inventory reservation silently lost" -- from a cause that has nothing to do with
 * recovery. The validator would return {@code DATA_INTEGRITY_FAILURE} for the wrong reason, and the
 * false-verdict rate in PRD section 18 would be unmeasurable because the workload was injecting its
 * own losses.
 *
 * <p>This class never throws for a produce failure; it resolves to a {@link ProduceOutcome} with
 * {@code durable=false}. Callers in a listener must then fail the listener so the offset is
 * <b>not</b> committed -- see {@code ChainListeners#requireDurable}.
 */
public class DurablePublisher {

    private static final Logger log = LoggerFactory.getLogger(DurablePublisher.class);

    private final KafkaTemplate<String, Object> template;
    private final AcknowledgementLedger ledger;
    private final Duration ackTimeout;

    public DurablePublisher(KafkaTemplate<String, Object> template,
                            AcknowledgementLedger ledger,
                            Duration ackTimeout) {
        this.template = template;
        this.ledger = ledger;
        this.ackTimeout = ackTimeout;
    }

    /**
     * Sends {@code payload} keyed by {@code key} to {@code topic}, then resolves with the durable or
     * rejected outcome. Bounded by the configured ack timeout so a wedged broker cannot hang the
     * caller indefinitely.
     */
    public CompletableFuture<ProduceOutcome> publish(String topic, String key, Object payload,
                                                     EventEnvelope envelope, String eventType) {
        CompletableFuture<ProduceOutcome> outcome;
        try {
            outcome = template.send(topic, key, payload)
                    .thenApply(result -> {
                        var md = result.getRecordMetadata();
                        ProduceOutcome ok = ProduceOutcome.durable(envelope, eventType, topic,
                                md.partition(), md.offset());
                        ledger.produced(envelope, eventType, topic, md.partition(), md.offset(), null);
                        return ok;
                    })
                    .exceptionally(ex -> {
                        ProduceOutcome bad = ProduceOutcome.rejected(envelope, eventType, topic,
                                classify(ex));
                        ledger.rejected(envelope, eventType, topic, bad.reason());
                        log.error("produce of {} for order {} was NOT acknowledged: {}",
                                eventType, envelope.orderId(), bad.reason());
                        return bad;
                    });
        } catch (RuntimeException e) {
            // send() can throw synchronously: metadata wait exceeding max.block.ms, a full buffer,
            // or a serialization failure. Without this branch the exception escapes the listener and
            // is handled by the container's error handler, which -- absent an explicit recoverer --
            // eventually logs and skips the record.
            ProduceOutcome bad = ProduceOutcome.rejected(envelope, eventType, topic, classify(e));
            ledger.rejected(envelope, eventType, topic, bad.reason());
            log.error("produce of {} for order {} failed synchronously: {}",
                    eventType, envelope.orderId(), bad.reason());
            return CompletableFuture.completedFuture(bad);
        }
        return outcome.orTimeout(ackTimeout.toMillis(), TimeUnit.MILLISECONDS)
                .exceptionally(ex -> {
                    String reason = (rootCause(ex) instanceof TimeoutException)
                            ? "ACK_TIMEOUT" : classify(ex);
                    ProduceOutcome bad = ProduceOutcome.rejected(envelope, eventType, topic, reason);
                    ledger.rejected(envelope, eventType, topic, reason);
                    log.error("produce of {} for order {} unresolved within {}ms: {}",
                            eventType, envelope.orderId(), ackTimeout.toMillis(), reason);
                    return bad;
                });
    }

    /**
     * Blocks until the outcome resolves and throws if the write was not durable, so that a Spring
     * Kafka container does <b>not</b> commit the consumed offset and the parent record is redelivered
     * instead of silently dropped.
     */
    public ProduceOutcome requireDurable(ProduceOutcome outcome) {
        if (outcome.durable()) {
            return outcome;
        }
        throw new IllegalStateException("downstream produce of " + outcome.eventType()
                + " for order " + outcome.orderId() + " was not acknowledged (" + outcome.reason()
                + "); refusing to commit offset so the record is redelivered");
    }

    public AcknowledgementLedger ledger() {
        return ledger;
    }

    static String classify(Throwable t) {
        Throwable root = rootCause(t);
        String name = root.getClass().getSimpleName();
        // NOT_ENOUGH_REPLICAS is worth naming specifically: it means min.insync.replicas did its job
        // and refused an acknowledgement the cluster could not honour.
        String msg = root.getMessage() == null ? "" : root.getMessage();
        if (msg.contains("NOT_ENOUGH_REPLICAS")) {
            return "NOT_ENOUGH_REPLICAS";
        }
        if (root instanceof TimeoutException) {
            return "ACK_TIMEOUT";
        }
        return name;
    }

    private static Throwable rootCause(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur;
    }
}
