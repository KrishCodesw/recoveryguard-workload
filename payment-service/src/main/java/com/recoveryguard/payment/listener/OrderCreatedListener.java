package com.recoveryguard.payment.listener;

import com.recoveryguard.events.EventEnvelope;
import com.recoveryguard.events.OrderCreatedEvent;
import com.recoveryguard.events.PaymentAuthorizedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String paymentsTopic;

    public OrderCreatedListener(KafkaTemplate<String, Object> kafkaTemplate,
                                 @Value("${recoveryguard.topics.payments}") String paymentsTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.paymentsTopic = paymentsTopic;
    }

    @KafkaListener(topics = "${recoveryguard.topics.orders}", groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCreated(OrderCreatedEvent event) {
        String orderId = event.envelope().orderId();
        log.info("Authorizing payment for order {}", orderId);

        // MVP: authorization always succeeds. This is intentionally simple --
        // the point of this service in the reference workload is the causal
        // chain and the acknowledged-event contract, not payment logic.
        PaymentAuthorizedEvent authorized = new PaymentAuthorizedEvent(
                EventEnvelope.of(orderId, "PaymentAuthorized"),
                UUID.randomUUID().toString(),
                event.totalAmount(),
                event.lines()
        );

        kafkaTemplate.send(paymentsTopic, orderId, authorized);
    }
}
