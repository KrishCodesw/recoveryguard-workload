package com.recoveryguard.payment.config;

import com.recoveryguard.events.KafkaContract;
import com.recoveryguard.events.OrderCreatedEvent;
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
 * Consumer wiring for Payment Service.
 *
 * <p>Only the type-specific parts live here; the contract settings come from {@link KafkaContract}
 * and the shared beans (producer factory, ledger, error handler, topic declarations, cluster guard)
 * come from {@code RecoveryGuardWorkloadAutoConfiguration}. That is deliberate: the previous version
 * of this class duplicated the entire producer configuration from order-service, and duplicated
 * contract settings are how the three services drift apart on what "acknowledged" means.
 *
 * <p>{@code AckMode.RECORD} combined with {@code enable.auto.commit=false} means an offset is
 * committed only after the listener returns normally. That is what makes the durability check in
 * {@code OrderCreatedListener} effective: if the downstream produce was not acknowledged the
 * listener throws, the offset is not committed, and the record is redelivered rather than dropped.
 */
@EnableKafka
@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ConsumerFactory<String, OrderCreatedEvent> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(
                KafkaContract.consumerProps(bootstrapServers, groupId, OrderCreatedEvent.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> kafkaListenerContainerFactory(
            ConsumerFactory<String, OrderCreatedEvent> consumerFactory,
            CommonErrorHandler recoveryGuardErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(recoveryGuardErrorHandler);
        return factory;
    }
}
