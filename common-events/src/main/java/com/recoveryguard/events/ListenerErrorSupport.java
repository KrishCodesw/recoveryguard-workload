package com.recoveryguard.events;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Error handling for chain listeners.
 *
 * <p><b>Why an explicit recoverer is required.</b> With no {@code CommonErrorHandler} configured,
 * Spring Kafka's {@code DefaultErrorHandler} applies: it seeks and retries a handful of times, then
 * <em>logs and skips</em> the record. Skipping is silent, unrecoverable event loss inside the
 * workload -- exactly the condition RecoveryGuard exists to detect, but caused by the workload
 * rather than by recovery. Worse, it is invisible: nothing records that a record was abandoned.
 *
 * <p>This routes exhausted records to {@code <topic>.DLT} and writes a {@code DROPPED} row to the
 * ledger, so every workload-side loss is attributable and the validation engine can exclude it from
 * expected state rather than reporting it as a data-integrity failure.
 */
public final class ListenerErrorSupport {

    private static final Logger log = LoggerFactory.getLogger(ListenerErrorSupport.class);

    /**
     * Retry attempts before a record is routed to the dead letter topic.
     *
     * <p>Deliberately generous. A chain listener throws when its downstream produce was not
     * acknowledged, which is the <em>expected</em> condition during an injected broker outage. With
     * a short backoff the record would be dead-lettered within a couple of seconds -- i.e. the
     * workload would abandon events precisely while the cluster was recovering, converting the very
     * condition under test into workload-side loss. The default window (60 x 2s = ~120s) matches
     * {@link KafkaContract#DELIVERY_TIMEOUT_MS} so the producer's own timeout, not the retry policy,
     * decides the outcome.
     */
    public static final long MAX_ATTEMPTS = 60L;
    /** Delay between retry attempts. */
    public static final long BACKOFF_INTERVAL_MS = 2_000L;

    private ListenerErrorSupport() {
    }

    /**
     * Builds an error handler that retries {@value #MAX_ATTEMPTS} times, then dead-letters the
     * record and records the abandonment in the ledger.
     */
    public static CommonErrorHandler errorHandler(KafkaTemplate<String, Object> template,
                                                  AcknowledgementLedger ledger) {
        return errorHandler(template, ledger, BACKOFF_INTERVAL_MS, MAX_ATTEMPTS);
    }

    /** Variant with an explicit retry window. */
    public static CommonErrorHandler errorHandler(KafkaTemplate<String, Object> template,
                                                  AcknowledgementLedger ledger,
                                                  long backoffIntervalMs, long maxAttempts) {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new org.apache.kafka.common.TopicPartition(
                        record.topic() + TopicContract.DLT_SUFFIX, record.partition()));

        return new DefaultErrorHandler((record, ex) -> {
            recordDropped(ledger, record, ex);
            try {
                recoverer.accept(record, ex);
            } catch (RuntimeException e) {
                // If dead-lettering also fails the record is genuinely unrecoverable. Say so loudly:
                // an unattributed loss is the worst possible outcome for an evidence-based system.
                log.error("FAILED TO DEAD-LETTER record from topic {} partition {} offset {} -- "
                                + "this loss is unattributed and the experiment should be treated as INCONCLUSIVE",
                        record.topic(), record.partition(), record.offset(), e);
                ledger.append(new AckRecord(AckRecord.SCHEMA_VERSION, AckRecord.UNSCOPED_EXPERIMENT,
                        "unknown", AckRecord.Role.DROPPED, null, null, "unknown",
                        record.topic(), record.partition(), record.offset(), null,
                        java.time.Instant.now(), null, "DLT_PUBLISH_FAILED:" + e.getClass().getSimpleName()));
            }
        }, new FixedBackOff(backoffIntervalMs, maxAttempts));
    }

    private static void recordDropped(AcknowledgementLedger ledger,
                                      ConsumerRecord<?, ?> record, Throwable ex) {
        String reason = DurablePublisher.classify(ex);
        log.warn("exhausted retries for topic {} partition {} offset {}; routing to DLT ({})",
                record.topic(), record.partition(), record.offset(), reason);
        // The payload is a deserialized business event when deserialization succeeded; when it did
        // not, the envelope is unavailable and the DLT row carries only Kafka coordinates.
        Object value = record.value();
        EventEnvelope envelope = extractEnvelope(value);
        ledger.append(new AckRecord(AckRecord.SCHEMA_VERSION, AckRecord.UNSCOPED_EXPERIMENT,
                "listener", AckRecord.Role.DROPPED,
                envelope == null ? null : envelope.eventId(),
                envelope == null ? String.valueOf(record.key()) : envelope.orderId(),
                envelope == null ? "unknown" : envelope.eventType(),
                record.topic(), record.partition(), record.offset(),
                envelope == null ? null : envelope.causationId(),
                java.time.Instant.now(), null, reason));
    }

    private static EventEnvelope extractEnvelope(Object value) {
        if (value instanceof OrderCreatedEvent e) {
            return e.envelope();
        }
        if (value instanceof PaymentAuthorizedEvent e) {
            return e.envelope();
        }
        if (value instanceof InventoryReservedEvent e) {
            return e.envelope();
        }
        return null;
    }

    /** Exposed for tests: the DLT topic a record from {@code topic} is routed to. */
    public static String dltFor(String topic) {
        return topic + TopicContract.DLT_SUFFIX;
    }
}
