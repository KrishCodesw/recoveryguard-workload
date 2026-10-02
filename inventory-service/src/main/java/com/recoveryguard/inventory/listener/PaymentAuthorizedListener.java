package com.recoveryguard.inventory.listener;

import com.recoveryguard.events.AcknowledgementLedger;
import com.recoveryguard.events.DeterministicIds;
import com.recoveryguard.events.DurablePublisher;
import com.recoveryguard.events.EventEnvelope;
import com.recoveryguard.events.InventoryReservedEvent;
import com.recoveryguard.events.PaymentAuthorizedEvent;
import com.recoveryguard.events.ProduceOutcome;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Consumes {@code PaymentAuthorized} and emits {@code InventoryReserved}.
 *
 * <p>The last hop of the Phase 1 chain, and the one RecoveryGuard's flagship scenario watches: a
 * missing {@code InventoryReserved} on an order that has a durable {@code OrderCreated} and
 * {@code PaymentAuthorized} is the canonical data-integrity failure. For that signal to mean
 * anything, this listener must never be the reason an event is missing -- hence the durability check
 * below, which the previous fire-and-forget {@code send()} did not have.
 */
@Component
public class PaymentAuthorizedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentAuthorizedListener.class);

    private final DurablePublisher publisher;
    private final AcknowledgementLedger ledger;
    private final String inventoryTopic;

    public PaymentAuthorizedListener(DurablePublisher publisher,
                                     AcknowledgementLedger ledger,
                                     @Value("${recoveryguard.topics.inventory}") String inventoryTopic) {
        this.publisher = publisher;
        this.ledger = ledger;
        this.inventoryTopic = inventoryTopic;
    }

    @KafkaListener(topics = "${recoveryguard.topics.payments}", groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentAuthorized(ConsumerRecord<String, PaymentAuthorizedEvent> record) {
        PaymentAuthorizedEvent event = record.value();
        String orderId = event.envelope().orderId();
        ledger.consumed(event.envelope(), record.topic(), record.partition(), record.offset());
        log.info("Reserving inventory for order {} (partition {} offset {})",
                orderId, record.partition(), record.offset());

        List<InventoryReservedEvent.ReservedLine> reserved = event.lines().stream()
                .map(l -> new InventoryReservedEvent.ReservedLine(l.sku(), l.quantity()))
                .toList();

        // MVP: no stock-check or backorder logic -- reservation always succeeds. Keeping this handler
        // simple isolates the variable that matters: does the write survive recovery.
        InventoryReservedEvent reservedEvent = new InventoryReservedEvent(
                EventEnvelope.childOf(event.envelope(), "InventoryReserved"),
                DeterministicIds.reservationId(orderId),
                reserved
        );

        ProduceOutcome outcome = publisher
                .publish(inventoryTopic, orderId, reservedEvent, reservedEvent.envelope(), "InventoryReserved")
                .join();
        // Throws when the broker did not acknowledge, which prevents the offset commit and causes
        // redelivery rather than silent loss.
        publisher.requireDurable(outcome);
    }
}
