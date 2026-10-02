package com.recoveryguard.events;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Append-only, newline-delimited JSON ledger of acknowledged (and rejected) events.
 *
 * <p>This is the independent ground truth RecoveryGuard validates against. Design constraints, in
 * priority order:
 *
 * <ol>
 *   <li><b>Durability over throughput.</b> Each record is flushed and, by default, {@code fsync}'d
 *       before the call returns. A ledger that can lose its tail is worse than no ledger: it turns
 *       "we never recorded the ack" into "the ack was recorded and then vanished", which is
 *       indistinguishable from the failure the project exists to detect. Disable {@code fsync} only
 *       for high-rate runs where you accept that tail loss.</li>
 *   <li><b>Outside Kafka.</b> The ledger file must not be written to a Kafka topic, or the same
 *       broker failure destroys both the event and the evidence that it existed.</li>
 *   <li><b>Never throws into the data path.</b> A ledger I/O failure must not silently convert a
 *       successful produce into an apparent loss. Failures are surfaced via {@link #lastError()}
 *       and logged; the caller decides. {@link #healthy()} reports whether the ledger is still
 *       trustworthy, so an experiment can be marked INCONCLUSIVE rather than quietly wrong.</li>
 * </ol>
 *
 * <p>Thread-safe: all appends are serialized on the instance lock.
 */
public final class AcknowledgementLedger implements AutoCloseable {

    private final Path file;
    private final String service;
    private final String experimentId;
    private final ObjectMapper mapper;
    private final boolean fsync;

    private FileChannel channel;
    private volatile boolean closed;
    private volatile String lastError;

    public AcknowledgementLedger(Path directory, String service, String experimentId, boolean fsync) {
        this.service = service;
        this.experimentId = (experimentId == null || experimentId.isBlank())
                ? AckRecord.UNSCOPED_EXPERIMENT : experimentId;
        this.mapper = RecoveryGuardJson.mapper();
        this.fsync = fsync;
        this.file = directory.resolve("ledger-" + service + ".jsonl");
        try {
            Files.createDirectories(directory);
            this.channel = FileChannel.open(file,
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            this.lastError = "OPEN_FAILED: " + e.getMessage();
            throw new UncheckedIOException("cannot open acknowledgement ledger at " + file, e);
        }
    }

    /** The ledger file this instance appends to. */
    public Path file() {
        return file;
    }

    /** {@code true} while the ledger has successfully persisted every record handed to it. */
    public boolean healthy() {
        return lastError == null && !closed;
    }

    /** The most recent persistence failure, or {@code null} if the ledger is healthy. */
    public String lastError() {
        return lastError;
    }

    /** Appends one record. Never throws; check {@link #healthy()} afterwards. */
    public void append(AckRecord record) {
        if (closed) {
            lastError = "APPEND_AFTER_CLOSE";
            return;
        }
        try {
            byte[] line = (mapper.writeValueAsString(record) + System.lineSeparator())
                    .getBytes(StandardCharsets.UTF_8);
            synchronized (this) {
                if (closed) {
                    lastError = "APPEND_AFTER_CLOSE";
                    return;
                }
                var buf = java.nio.ByteBuffer.wrap(line);
                while (buf.hasRemaining()) {
                    channel.write(buf);
                }
                if (fsync) {
                    // force(false) flushes data without requiring a metadata sync; sufficient for
                    // an append-only file whose length is recovered from the filesystem.
                    channel.force(false);
                }
            }
        } catch (IOException | RuntimeException e) {
            // Deliberately swallowed: a ledger failure must not masquerade as event loss.
            lastError = e.getClass().getSimpleName() + ": " + e.getMessage();
        }
    }

    /** Convenience: record a broker-acknowledged produce. */
    public void produced(EventEnvelope envelope, String eventType, String topic,
                         int partition, long offset, Integer isrSize) {
        append(new AckRecord(AckRecord.SCHEMA_VERSION, experimentId, service, AckRecord.Role.PRODUCED,
                envelope.eventId(), envelope.orderId(), eventType, topic, partition, offset,
                envelope.causationId(), java.time.Instant.now(), isrSize, null));
    }

    /** Convenience: record a produce that never became durable. */
    public void rejected(EventEnvelope envelope, String eventType, String topic, String reason) {
        append(new AckRecord(AckRecord.SCHEMA_VERSION, experimentId, service, AckRecord.Role.REJECTED,
                envelope.eventId(), envelope.orderId(), eventType, topic, null, null,
                envelope.causationId(), java.time.Instant.now(), null, reason));
    }

    /** Convenience: record a listener delivery. */
    public void consumed(EventEnvelope envelope, String topic, int partition, long offset) {
        append(new AckRecord(AckRecord.SCHEMA_VERSION, experimentId, service, AckRecord.Role.CONSUMED,
                envelope.eventId(), envelope.orderId(), envelope.eventType(), topic, partition, offset,
                envelope.causationId(), java.time.Instant.now(), null, null));
    }

    /** Convenience: record a workload-side abandonment routed to a dead letter topic. */
    public void dropped(EventEnvelope envelope, String topic, String reason) {
        append(new AckRecord(AckRecord.SCHEMA_VERSION, experimentId, service, AckRecord.Role.DROPPED,
                envelope == null ? null : envelope.eventId(),
                envelope == null ? null : envelope.orderId(),
                envelope == null ? "unknown" : envelope.eventType(),
                topic, null, null,
                envelope == null ? null : envelope.causationId(),
                java.time.Instant.now(), null, reason));
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        try {
            if (channel != null && channel.isOpen()) {
                channel.force(true);
                channel.close();
            }
        } catch (IOException e) {
            lastError = "CLOSE_FAILED: " + e.getMessage();
        }
    }
}
