package com.recoveryguard.payment;

import com.recoveryguard.events.DeterministicIds;
import com.recoveryguard.events.EventEnvelope;
import com.recoveryguard.events.OrderCreatedEvent;
import com.recoveryguard.events.OrderLine;
import com.recoveryguard.events.PaymentAuthorizedEvent;
import com.recoveryguard.events.RecoveryGuardJson;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end proof that the Order -> Payment hop preserves the evidence RecoveryGuard depends on.
 *
 * <p>Runs a real embedded broker and the real Payment Service context, so it exercises the actual
 * listener, the actual container error handler, the actual ledger writer and the actual serializer --
 * not mocks. It is the regression test for the three defects that previously made the workload an
 * unreliable system under test:
 *
 * <ul>
 *   <li><b>fire-and-forget produce</b> -- the child event is now only considered done once the broker
 *       has acknowledged it;</li>
 *   <li><b>random derived ids</b> -- the payment id is now deterministic, so a redelivery produces an
 *       identical record instead of one that looks like replica divergence;</li>
 *   <li><b>no ground truth</b> -- every hop is now written to an append-only ledger outside Kafka.</li>
 * </ul>
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.group-id=payment-service-it",
        "recoveryguard.topics.orders=orders",
        "recoveryguard.topics.payments=payments",
        // A single embedded broker cannot satisfy the experiment-profile contract; the guard is a
        // startup check, not part of what this test asserts.
        "recoveryguard.cluster.guard-enabled=false",
        "recoveryguard.cluster.profile=dev",
        "recoveryguard.ledger.fsync=false",
        "recoveryguard.ledger.path=./target/it-runs",
        "recoveryguard.experiment-id=it-payment-chain"
})
@EmbeddedKafka(partitions = 1, topics = {"orders", "payments", "orders.DLT", "payments.DLT"})
@DirtiesContext
class PaymentChainIntegrationTest {

    private static final ObjectMapper MAPPER = RecoveryGuardJson.mapper();

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    @DisplayName("an OrderCreated produces a causally linked, deterministically identified PaymentAuthorized, recorded in the ledger")
    void chainHopPreservesEvidence() throws Exception {
        String orderId = UUID.randomUUID().toString();
        OrderCreatedEvent orderCreated = new OrderCreatedEvent(
                EventEnvelope.of(orderId, "OrderCreated"),
                "cust-1",
                List.of(new OrderLine("SKU-1", 2, new BigDecimal("19.99"))),
                new BigDecimal("39.98"));

        kafkaTemplate.send("orders", orderId, orderCreated).get(30, java.util.concurrent.TimeUnit.SECONDS);

        PaymentAuthorizedEvent authorized = consumeOne("payments", orderId, Duration.ofSeconds(60));

        // --- causality -------------------------------------------------------
        assertThat(authorized.envelope().orderId()).isEqualTo(orderId);
        assertThat(authorized.envelope().eventType()).isEqualTo("PaymentAuthorized");
        assertThat(authorized.envelope().schemaVersion()).isEqualTo(EventEnvelope.SCHEMA_VERSION);
        // The link back to the causing event is what lets the validator distinguish a redelivery
        // from genuine divergence.
        assertThat(authorized.envelope().causationId())
                .as("causationId must be the parent OrderCreated's eventId")
                .isEqualTo(orderCreated.envelope().eventId());
        assertThat(authorized.envelope().eventId()).isNotEqualTo(orderCreated.envelope().eventId());

        // --- deterministic identity -----------------------------------------
        assertThat(authorized.paymentId()).isEqualTo(DeterministicIds.paymentId(orderId));
        assertThat(authorized.authorizedAmount()).isEqualByComparingTo("39.98");
        assertThat(authorized.lines()).hasSize(1);

        // --- the payload actually crossed the wire intact --------------------
        assertThat(authorized.envelope().producedAt()).isNotNull();

        // --- ground truth ----------------------------------------------------
        Path ledgerFile = Path.of("./target/it-runs", "it-payment-chain", "ledger-payment-service.jsonl");
        assertThat(ledgerFile)
                .as("the acknowledgement ledger must exist outside Kafka")
                .exists();

        List<String> lines = Files.readAllLines(ledgerFile);
        List<JsonNode> rows = new java.util.ArrayList<>();
        for (String line : lines) {
            JsonNode row = MAPPER.readTree(line);
            if (orderId.equals(row.path("orderId").asText())) {
                rows.add(row);
            }
        }
        assertThat(rows)
                .as("expected a CONSUMED row for the parent and a PRODUCED row for the child")
                .isNotEmpty();
        assertThat(rows).extracting(r -> r.get("role").asText())
                .contains("CONSUMED", "PRODUCED");

        JsonNode produced = rows.stream()
                .filter(r -> "PRODUCED".equals(r.get("role").asText()))
                .findFirst().orElseThrow();
        assertThat(produced.get("topic").asText()).isEqualTo("payments");
        assertThat(produced.get("eventType").asText()).isEqualTo("PaymentAuthorized");
        assertThat(produced.get("offset").asLong()).isGreaterThanOrEqualTo(0L);
        assertThat(produced.get("causationId").asText())
                .isEqualTo(orderCreated.envelope().eventId().toString());
        assertThat(produced.get("experimentId").asText()).isEqualTo("it-payment-chain");
    }

    @Test
    @DisplayName("timestamps on the wire are exact ISO-8601, not lossy epoch decimals")
    void wireFormatIsExact() throws Exception {
        String orderId = UUID.randomUUID().toString();
        OrderCreatedEvent orderCreated = new OrderCreatedEvent(
                EventEnvelope.of(orderId, "OrderCreated"), "cust-2",
                List.of(new OrderLine("SKU-9", 1, new BigDecimal("10.00"))),
                new BigDecimal("10.00"));

        kafkaTemplate.send("orders", orderId, orderCreated).get(30, java.util.concurrent.TimeUnit.SECONDS);
        PaymentAuthorizedEvent authorized = consumeOne("payments", orderId, Duration.ofSeconds(60));

        String json = MAPPER.writeValueAsString(authorized);
        assertThat(json).contains("\"producedAt\":\"");
        assertThat(json).matches("(?s).*\"producedAt\":\"\\d{4}-\\d{2}-\\d{2}T[\\d:.]+Z\".*");
        assertThat(authorized.envelope().producedAt().getNano())
                .as("nanosecond precision must survive the round trip")
                .isNotZero();
    }

    /**
     * Reads until a record for {@code expectedOrderId} appears.
     *
     * <p>Filtering by order id is required for isolation: the observer consumer starts at
     * {@code earliest} with a fresh group, so it sees every record ever written to the topic,
     * including ones produced by other tests in this class.
     */
    private PaymentAuthorizedEvent consumeOne(String topic, String expectedOrderId, Duration timeout) {
        String brokers = System.getProperty("spring.embedded.kafka.brokers");
        assertThat(brokers).as("embedded broker address must be published").isNotBlank();
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-observer-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.recoveryguard.events");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, PaymentAuthorizedEvent.class);

        ConsumerFactory<String, PaymentAuthorizedEvent> cf = new DefaultKafkaConsumerFactory<>(props);
        Consumer<String, PaymentAuthorizedEvent> consumer = cf.createConsumer();
        try {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                for (ConsumerRecord<String, PaymentAuthorizedEvent> r
                        : consumer.poll(Duration.ofMillis(500))) {
                    if (expectedOrderId.equals(r.value().envelope().orderId())) {
                        return r.value();
                    }
                }
            }
            throw new AssertionError("no " + topic + " record for order " + expectedOrderId
                    + " within " + timeout);
        } finally {
            consumer.close();
        }
    }

}
