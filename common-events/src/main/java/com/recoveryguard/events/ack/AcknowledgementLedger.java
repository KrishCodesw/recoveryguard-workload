package com.recoveryguard.events.ack;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.recoveryguard.events.EventEnvelope;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * Append-only acknowledgement ledger for the reference workload.
 *
 * A record is appended only after the Kafka producer future completes successfully.
 * The implementation is intentionally dependency-light so every workload service can
 * use the same evidence format without introducing a database or another service.
 */
public final class AcknowledgementLedger {

    private final Path path;
    private final String experimentId;
    private final String producerService;
    private final ObjectMapper objectMapper;
    private final Object lock = new Object();

    public AcknowledgementLedger(Path path, String experimentId, String producerService) {
        this.path = path;
        this.experimentId = experimentId;
        this.producerService = producerService;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    public void record(EventEnvelope envelope, RecordMetadata metadata) {
        AcknowledgementRecord record = new AcknowledgementRecord(
                experimentId,
                producerService,
                envelope.eventId(),
                envelope.orderId(),
                envelope.eventType(),
                metadata.topic(),
                metadata.partition(),
                metadata.offset(),
                envelope.producedAt(),
                Instant.now()
        );

        try {
            String json = objectMapper.writeValueAsString(record) + System.lineSeparator();
            synchronized (lock) {
                Path parent = path.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Files.writeString(
                        path,
                        json,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.APPEND
                );
            }
        } catch (IOException e) {
            // Do not silently convert an evidence-collection failure into an
            // application success. The produced event is acknowledged, but the
            // acknowledgement evidence could not be persisted.
            throw new IllegalStateException("Unable to persist acknowledgement ledger at " + path, e);
        }
    }
}
