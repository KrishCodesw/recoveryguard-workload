package com.recoveryguard.inventory.listener;

import com.recoveryguard.events.EventEnvelope;
import com.recoveryguard.events.InventoryReservedEvent;
import com.recoveryguard.events.PaymentAuthorizedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class PaymentAuthorizedListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentAuthorizedListener.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String inventoryTopic;

    public PaymentAuthorizedListener(KafkaTemplate<String, Object> kafkaTemplate,
                                      @Value("${recoveryguard.topics.inventory}") String inventoryTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.inventoryTopic = inventoryTopic;
    }

    @KafkaListener(topics = "${recoveryguard.topics.payments}", groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentAuthorized(PaymentAuthorizedEvent event) {
        String orderId = event.envelope().orderId();
        log.info("Reserving inventory for order {}", orderId);

        List<InventoryReservedEvent.ReservedLine> reserved = event.lines().stream()
                .map(l -> new InventoryReservedEvent.ReservedLine(l.sku(), l.quantity()))
                .toList();

        // MVP: no stock-check / backorder logic -- reservation always
        // succeeds. This topic ("inventory") is where the flagship failure
        // scenario (unclean leader election) is injected in
        // recoveryguard-failures, so keeping this handler simple isolates
        // the variable that matters: does the write survive recovery.
        InventoryReservedEvent reservedEvent = new InventoryReservedEvent(
                EventEnvelope.of(orderId, "InventoryReserved"),
                UUID.randomUUID().toString(),
                reserved
        );

        kafkaTemplate.send(inventoryTopic, orderId, reservedEvent);
    }
}
