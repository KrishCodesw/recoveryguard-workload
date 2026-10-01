package com.recoveryguard.order;

import com.recoveryguard.events.EventEnvelope;
import com.recoveryguard.events.ProduceOutcome;
import com.recoveryguard.order.service.OrderPublisher;
import com.recoveryguard.order.web.OrderAckResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The HTTP ack surface is how an experiment driver learns what it created, so its shape is part of
 * the contract -- not an implementation detail.
 */
class OrderAckResponseTest {

    @Test
    @DisplayName("a durable outcome surfaces orderId, eventId and broker coordinates")
    void durableOutcomeExposesEverything() {
        EventEnvelope e = EventEnvelope.of("order-1", "OrderCreated");
        ProduceOutcome outcome = ProduceOutcome.durable(e, "OrderCreated", "orders", 2, 4117L);

        OrderAckResponse response = OrderAckResponse.from(outcome);

        assertThat(response.durable()).isTrue();
        assertThat(response.orderId()).isEqualTo("order-1");
        assertThat(response.eventId()).isEqualTo(e.eventId());
        assertThat(response.topic()).isEqualTo("orders");
        assertThat(response.partition()).isEqualTo(2);
        assertThat(response.offset()).isEqualTo(4117L);
        assertThat(response.reason()).isNull();
    }

    @Test
    @DisplayName("a rejected outcome still carries the orderId, so the driver can record a negative ack")
    void rejectedOutcomeKeepsOrderId() {
        EventEnvelope e = EventEnvelope.of("order-2", "OrderCreated");
        ProduceOutcome outcome = ProduceOutcome.rejected(e, "OrderCreated", "orders", "NOT_ENOUGH_REPLICAS");

        OrderAckResponse response = OrderAckResponse.from(outcome);

        assertThat(response.durable()).isFalse();
        // Without the orderId a rejected request is indistinguishable from an unknown one, and the
        // driver cannot tell "never written" from "maybe written".
        assertThat(response.orderId()).isEqualTo("order-2");
        assertThat(response.partition()).isNull();
        assertThat(response.offset()).isNull();
        assertThat(response.reason()).isEqualTo("NOT_ENOUGH_REPLICAS");
    }

    @Test
    @DisplayName("money is normalized to 2dp with HALF_UP, so serialized amounts are stable")
    void moneyIsNormalized() {
        assertThat(OrderPublisher.scale(new BigDecimal("19.994")))
                .isEqualByComparingTo(new BigDecimal("19.99").setScale(2, RoundingMode.HALF_UP));
        assertThat(OrderPublisher.scale(new BigDecimal("19.995")))
                .isEqualByComparingTo(new BigDecimal("20.00"));
        assertThat(OrderPublisher.scale(new BigDecimal("39.98")).scale()).isEqualTo(2);
    }
}
