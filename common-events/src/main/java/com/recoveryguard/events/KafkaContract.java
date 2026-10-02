package com.recoveryguard.events;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * The single definition of the producer and consumer settings that constitute RecoveryGuard's
 * "acknowledged event" contract.
 *
 * <p><b>These are not defaults and must not be relaxed.</b> They define what the word
 * "acknowledged" means everywhere else in the project, including in every verdict recoveryguard-core
 * produces. Weakening {@code acks} or disabling idempotence to improve throughput silently
 * invalidates the entire evidence model.
 *
 * <p><b>Why the timeouts are explicit.</b> They were previously left at Kafka's defaults
 * ({@code max.block.ms} 60s, {@code delivery.timeout.ms} 120s, {@code request.timeout.ms} 30s).
 * During an injected failure the duration a producer keeps retrying <em>is</em> part of the
 * experiment: it determines whether a record is eventually acknowledged, rejected, or lost. Leaving
 * it implicit makes the experiment irreproducible across client versions and impossible to record
 * in the run manifest. Every value here is surfaced by {@link #describe()} so the workload driver
 * can persist it.
 */
public final class KafkaContract {

    /** Total time a produce may spend retrying before it is failed to the caller. */
    public static final int DELIVERY_TIMEOUT_MS = 120_000;
    /** Per-request broker timeout. */
    public static final int REQUEST_TIMEOUT_MS = 30_000;
    /** How long {@code send()} may block waiting for metadata or buffer space. */
    public static final int MAX_BLOCK_MS = 60_000;
    /** In-flight requests per connection; 5 is the maximum that preserves ordering with idempotence. */
    public static final int MAX_IN_FLIGHT = 5;

    private KafkaContract() {
    }

    /**
     * Producer properties implementing the acknowledged-event contract.
     *
     * <p>Note what is deliberately <em>absent</em>: {@code retries} is not set, because with
     * idempotence enabled the effective retry budget is already bounded by
     * {@code delivery.timeout.ms}. Setting {@code retries=MAX_VALUE} alongside it was redundant and
     * misleading -- it implied unbounded retry where the timeout is what actually governs.
     */
    public static Map<String, Object> producerProps(String bootstrapServers) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // VALUE_SERIALIZER_CLASS_CONFIG is intentionally omitted: each service installs a
        // JsonSerializer built from RecoveryGuardJson.mapper() so the wire format is pinned.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, DELIVERY_TIMEOUT_MS);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, REQUEST_TIMEOUT_MS);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, MAX_BLOCK_MS);
        return props;
    }

    /**
     * Consumer properties for a chain listener. Auto-commit is off: an offset must never advance
     * past a record whose downstream effect has not yet been durably acknowledged.
     */
    public static Map<String, Object> consumerProps(String bootstrapServers, String groupId) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        // ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS and TRUSTED_PACKAGES are set per-service,
        // because the concrete event type differs at each hop.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return props;
    }

    /**
     * Consumer properties for a chain listener bound to a concrete event type.
     *
     * <p>{@code VALUE_DEFAULT_TYPE} is pinned per service rather than relying on the {@code __TypeId__}
     * record header, so a record whose header is missing or from an untrusted package still
     * deserializes predictably instead of falling back to a LinkedHashMap and failing deep inside a
     * listener.
     */
    public static Map<String, Object> consumerProps(String bootstrapServers, String groupId,
                                                    Class<?> valueType) {
        Map<String, Object> props = consumerProps(bootstrapServers, groupId);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS,
                org.springframework.kafka.support.serializer.JsonDeserializer.class);
        props.put(org.springframework.kafka.support.serializer.JsonDeserializer.TRUSTED_PACKAGES,
                "com.recoveryguard.events");
        props.put(org.springframework.kafka.support.serializer.JsonDeserializer.VALUE_DEFAULT_TYPE,
                valueType);
        return props;
    }

    /** Human- and machine-readable snapshot of the contract, for the experiment run manifest. */
    public static Map<String, Object> describe() {
        return Map.of(
                "acks", "all",
                "enableIdempotence", true,
                "maxInFlightRequestsPerConnection", MAX_IN_FLIGHT,
                "deliveryTimeoutMs", DELIVERY_TIMEOUT_MS,
                "requestTimeoutMs", REQUEST_TIMEOUT_MS,
                "maxBlockMs", MAX_BLOCK_MS,
                "enableAutoCommit", false,
                "isolationLevel", "read_committed",
                "autoOffsetReset", "earliest"
        );
    }
}
