package com.recoveryguard.inventory.config;

import com.recoveryguard.events.KafkaContract;
import com.recoveryguard.events.PaymentAuthorizedEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * Consumer wiring for Inventory Service. See {@code payment-service}'s {@code KafkaConfig} for the
 * rationale; the two are intentionally identical apart from the event type.
 *
 * <p>This is the hop the flagship failure scenario targets: an order with a durable
 * {@code OrderCreated} and {@code PaymentAuthorized} but no {@code InventoryReserved} after recovery
 * is the canonical data-integrity failure for this workload. Keeping this listener's own semantics
 * strictly at-least-once and fully attributed is what makes that observation trustworthy.
 */
@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, PaymentAuthorizedEvent> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                KafkaContract.consumerProps(bootstrapServers, groupId, PaymentAuthorizedEvent.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, PaymentAuthorizedEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, PaymentAuthorizedEvent> consumerFactory,
            CommonErrorHandler recoveryGuardErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, PaymentAuthorizedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(recoveryGuardErrorHandler);
        return factory;
    }
}
