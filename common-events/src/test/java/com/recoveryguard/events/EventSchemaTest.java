package com.recoveryguard.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Locks the wire format of business events.
 *
 * <p>This exists because the format was previously an accident of Spring Kafka's default mapper.
 * That default enabled {@code WRITE_DATES_AS_TIMESTAMPS}, so {@code producedAt} went on the wire as
 * {@code 1790837640.123456789} -- an epoch decimal. Any consumer parsing that as a JSON number gets
 * a float64, which cannot represent 19 significant digits, so sub-microsecond precision was silently
 * discarded. For a project whose measurement plan is built on detection, evidence-collection and
 * validation latencies, lossy timestamps corrupt the recovery timeline that justifies every verdict.
 *
 * <p>These assertions fail if a dependency upgrade quietly changes the format.
 */
class EventSchemaTest {

    private static final ObjectMapper MAPPER = RecoveryGuardJson.mapper();

    private static final Instant FIXED = Instant.parse("2026-10-01T06:54:00.123456789Z");

    private OrderCreatedEvent sample() {
        var envelope = new EventEnvelope(EventEnvelope.SCHEMA_VERSION,
                UUID.fromString("11111111-2222-3333-4444-555555555555"),
                "order-abc", "OrderCreated", null, FIXED);
        return new OrderCreatedEvent(envelope, "cust-1",
                List.of(new OrderLine("SKU-1", 2, new BigDecimal("19.99"))),
                new BigDecimal("39.98"));
    }

    @Test
    @DisplayName("timestamps are ISO-8601 with full nanosecond precision, not epoch decimals")
    void timestampsAreIso8601() throws Exception {
        String json = MAPPER.writeValueAsString(sample());

        assertThat(json).contains("\"producedAt\":\"2026-10-01T06:54:00.123456789Z\"");
        assertThat(json).doesNotContain("1790837640");
    }

    @Test
    @DisplayName("survives a round trip without losing precision")
    void roundTrips() throws Exception {
        OrderCreatedEvent original = sample();
        String json = MAPPER.writeValueAsString(original);
        OrderCreatedEvent parsed = MAPPER.readValue(json, OrderCreatedEvent.class);

        assertThat(parsed.envelope().producedAt()).isEqualTo(FIXED);
        assertThat(parsed.envelope().producedAt().getNano()).isEqualTo(123456789);
        assertThat(parsed).isEqualTo(original);
    }

    @Test
    @DisplayName("carries schemaVersion, and omits a null causationId rather than emitting null")
    void schemaVersionAndNullHandling() throws Exception {
        String json = MAPPER.writeValueAsString(sample());

        assertThat(json).contains("\"schemaVersion\":1");
        assertThat(json).doesNotContain("causationId");
    }

    @Test
    @DisplayName("childOf links causationId to the parent eventId and preserves the orderId")
    void childOfLinksCausation() {
        EventEnvelope parent = EventEnvelope.of("order-1", "OrderCreated");
        EventEnvelope child = EventEnvelope.childOf(parent, "PaymentAuthorized");

        assertThat(child.causationId()).isEqualTo(parent.eventId());
        assertThat(child.orderId()).isEqualTo(parent.orderId());
        assertThat(child.eventId()).isNotEqualTo(parent.eventId());
        assertThat(child.schemaVersion()).isEqualTo(EventEnvelope.SCHEMA_VERSION);
    }

    @Test
    @DisplayName("money keeps its scale in the serialized form")
    void moneyScaleIsStable() throws Exception {
        String json = MAPPER.writeValueAsString(sample());
        assertThat(json).contains("\"unitPrice\":19.99").contains("\"totalAmount\":39.98");
    }
}
