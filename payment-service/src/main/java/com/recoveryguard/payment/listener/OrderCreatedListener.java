package com.recoveryguard.payment.listener;

import com.recoveryguard.events.AcknowledgementLedger;
import com.recoveryguard.events.DeterministicIds;
import com.recoveryguard.events.DurablePublisher;
import com.recoveryguard.events.EventEnvelope;
import com.recoveryguard.events.OrderCreatedEvent;
import com.recoveryguard.events.PaymentAuthorizedEvent;
import com.recoveryguard.events.ProduceOutcome;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code OrderCreated} and emits {@code PaymentAuthorized}.
 *
 * <p><b>The durability check is the point of this class.</b> It previously called
 * {@code kafkaTemplate.send(...)} and discarded the returned future. Because the container runs in
 * {@code AckMode.RECORD}, the consumed offset was committed the moment the {@code void} method
 * returned -- before any broker had acknowledged the child event. If that produce then failed during
 * an outage, the parent offset was already committed, the record was never redelivered, and the
 * {@code PaymentAuthorized} simply ceased to exist.
 *
 * <p>That is a workload-side loss wearing the costume of a recovery-side one. Now the listener waits
 * for the broker's answer and throws if the write was not durable, so the offset is not committed and
 * the record is redelivered instead.
 */
@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final DurablePublisher publisher;
    private final AcknowledgementLedger ledger;
    private final String paymentsTopic;

    public OrderCreatedListener(DurablePublisher publisher,
                                AcknowledgementLedger ledger,
                                @Value("${recoveryguard.topics.payments}") String paymentsTopic) {
        this.publisher = publisher;
        this.ledger = ledger;
        this.paymentsTopic = paymentsTopic;
    }

    @KafkaListener(topics = "${recoveryguard.topics.orders}", groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCreated(ConsumerRecord<String, OrderCreatedEvent> record) {
        OrderCreatedEvent event = record.value();
        String orderId = event.envelope().orderId();
        ledger.consumed(event.envelope(), record.topic(), record.partition(), record.offset());
        log.info("Authorizing payment for order {} (partition {} offset {})",
                orderId, record.partition(), record.offset());

        // MVP: authorization always succeeds. This service exists to exercise the causal chain and
        // the acknowledged-event contract, not to model payment logic.
        PaymentAuthorizedEvent authorized = new PaymentAuthorizedEvent(
                // childOf carries the parent eventId as causationId, which is what lets the
                // validation engine tell a redelivery apart from genuine replica divergence.
                EventEnvelope.childOf(event.envelope(), "PaymentAuthorized"),
                // Deterministic: under at-least-once delivery a redelivered OrderCreated produces an
                // identical paymentId, so duplicates collapse instead of looking like corruption.
                DeterministicIds.paymentId(orderId),
                event.totalAmount(),
                event.lines()
        );

        ProduceOutcome outcome = publisher
                .publish(paymentsTopic, orderId, authorized, authorized.envelope(), "PaymentAuthorized")
                .join();
        // Throws when the broker did not acknowledge, which prevents the offset commit.
        publisher.requireDurable(outcome);
    }
}
