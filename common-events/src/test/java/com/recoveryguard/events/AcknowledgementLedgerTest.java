package com.recoveryguard.events;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The ledger is the independent ground truth RecoveryGuard validates against, so its durability and
 * format are load-bearing: a ledger that can lose its tail turns "never recorded" into "recorded
 * then vanished", which is indistinguishable from the event loss the project exists to detect.
 */
class AcknowledgementLedgerTest {

    private static final ObjectMapper MAPPER = RecoveryGuardJson.mapper();

    @Test
    @DisplayName("appends one parseable JSON line per record and stays healthy")
    void appendsJsonLines(@TempDir Path dir) throws Exception {
        try (AcknowledgementLedger ledger = new AcknowledgementLedger(dir, "payment-service", "exp-1", true)) {
            EventEnvelope e = EventEnvelope.of("order-1", "PaymentAuthorized");
            ledger.produced(e, "PaymentAuthorized", "payments", 2, 4117L, 3);
            ledger.consumed(e, "orders", 2, 99L);
            ledger.rejected(e, "PaymentAuthorized", "payments", "NOT_ENOUGH_REPLICAS");

            assertThat(ledger.healthy()).isTrue();
            assertThat(ledger.lastError()).isNull();
        }

        Path file = dir.resolve("ledger-payment-service.jsonl");
        List<String> lines = Files.readAllLines(file);
        assertThat(lines).hasSize(3);

        JsonNode first = MAPPER.readTree(lines.get(0));
        assertThat(first.get("role").asText()).isEqualTo("PRODUCED");
        assertThat(first.get("experimentId").asText()).isEqualTo("exp-1");
        assertThat(first.get("service").asText()).isEqualTo("payment-service");
        assertThat(first.get("topic").asText()).isEqualTo("payments");
        assertThat(first.get("partition").asInt()).isEqualTo(2);
        assertThat(first.get("offset").asLong()).isEqualTo(4117L);
        assertThat(first.get("isrSize").asInt()).isEqualTo(3);
        assertThat(first.get("schemaVersion").asInt()).isEqualTo(AckRecord.SCHEMA_VERSION);
        // Timestamps must be exact strings, not lossy epoch decimals.
        assertThat(first.get("at").asText()).contains("T").endsWith("Z");

        assertThat(MAPPER.readTree(lines.get(1)).get("role").asText()).isEqualTo("CONSUMED");
        JsonNode rejected = MAPPER.readTree(lines.get(2));
        assertThat(rejected.get("role").asText()).isEqualTo("REJECTED");
        assertThat(rejected.get("reason").asText()).isEqualTo("NOT_ENOUGH_REPLICAS");
    }

    @Test
    @DisplayName("a rejected record is not durable, so it never enters expected state")
    void rejectedIsNotDurable(@TempDir Path dir) {
        AckRecord rejected = new AckRecord(AckRecord.SCHEMA_VERSION, "exp", "s", AckRecord.Role.REJECTED,
                UUID.randomUUID(), "o", "OrderCreated", "orders", null, null, null,
                Instant.now(), null, "ACK_TIMEOUT");
        AckRecord produced = new AckRecord(AckRecord.SCHEMA_VERSION, "exp", "s", AckRecord.Role.PRODUCED,
                UUID.randomUUID(), "o", "OrderCreated", "orders", 0, 1L, null,
                Instant.now(), 3, null);

        assertThat(rejected.isDurable()).isFalse();
        assertThat(produced.isDurable()).isTrue();
    }

    @Test
    @DisplayName("creates the experiment directory, appends across reopens, and closes idempotently")
    void createsDirAndAppendsAcrossReopen(@TempDir Path dir) throws Exception {
        Path scoped = dir.resolve("exp-42");
        try (AcknowledgementLedger l1 = new AcknowledgementLedger(scoped, "order-service", "exp-42", true)) {
            l1.produced(EventEnvelope.of("o1", "OrderCreated"), "OrderCreated", "orders", 0, 0L, 1);
        }
        assertThat(scoped).exists();
        try (AcknowledgementLedger l2 = new AcknowledgementLedger(scoped, "order-service", "exp-42", true)) {
            l2.produced(EventEnvelope.of("o2", "OrderCreated"), "OrderCreated", "orders", 0, 1L, 1);
            l2.close();
            l2.close();
            // Appending after close must not throw into the data path.
            l2.produced(EventEnvelope.of("o3", "OrderCreated"), "OrderCreated", "orders", 0, 2L, 1);
            assertThat(l2.healthy()).isFalse();
            assertThat(l2.lastError()).isEqualTo("APPEND_AFTER_CLOSE");
        }
        assertThat(Files.readAllLines(scoped.resolve("ledger-order-service.jsonl"))).hasSize(2);
    }

    @Test
    @DisplayName("a blank experiment id falls back to the unscoped sentinel in every row")
    void blankExperimentIdFallsBack(@TempDir Path dir) throws Exception {
        // The ledger does not create the experiment subdirectory itself -- the caller (the shared
        // auto-configuration) does, so that a service pointed at an explicit directory is honoured.
        // What the ledger owns is stamping every row with the resolved experiment id.
        try (AcknowledgementLedger ledger = new AcknowledgementLedger(dir, "svc", "   ", true)) {
            ledger.produced(EventEnvelope.of("o1", "OrderCreated"), "OrderCreated", "orders", 0, 0L, 1);
        }
        JsonNode row = MAPPER.readTree(Files.readAllLines(dir.resolve("ledger-svc.jsonl")).get(0));
        assertThat(row.get("experimentId").asText()).isEqualTo(AckRecord.UNSCOPED_EXPERIMENT);
    }

    @Test
    @DisplayName("concurrent appends do not interleave or lose lines")
    void concurrentAppendsAreSafe(@TempDir Path dir) throws Exception {
        int threads = 8;
        int perThread = 250;
        try (AcknowledgementLedger ledger = new AcknowledgementLedger(dir, "svc", "exp", false)) {
            List<Thread> ts = new java.util.ArrayList<>();
            for (int t = 0; t < threads; t++) {
                Thread th = new Thread(() -> {
                    for (int i = 0; i < perThread; i++) {
                        ledger.produced(EventEnvelope.of("o" + i, "OrderCreated"),
                                "OrderCreated", "orders", 0, i, 1);
                    }
                });
                ts.add(th);
                th.start();
            }
            for (Thread th : ts) {
                th.join();
            }
            assertThat(ledger.healthy()).isTrue();
        }
        List<String> lines = Files.readAllLines(dir.resolve("ledger-svc.jsonl"));
        assertThat(lines).hasSize(threads * perThread);
        // Every line must be independently parseable -- interleaved writes would break this.
        for (String line : lines) {
            assertThat(MAPPER.readTree(line).get("role").asText()).isEqualTo("PRODUCED");
        }
    }
}
