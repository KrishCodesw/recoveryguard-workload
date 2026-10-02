package com.recoveryguard.events;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Guards the acknowledged-event contract.
 *
 * <p>These settings are the definition of "acknowledged" for the whole project, including every
 * verdict recoveryguard-core produces. Relaxing {@code acks} or enabling auto-commit silently
 * invalidates the evidence model, so a change here should fail a test rather than slip through review.
 */
class KafkaContractTest {

    private final Map<String, Object> producer = KafkaContract.producerProps("localhost:9092");
    private final Map<String, Object> consumer = KafkaContract.consumerProps("localhost:9092", "g");

    @Test
    @DisplayName("producer requires all ISR replicas and idempotence")
    void producerContract() {
        assertThat(producer.get(ProducerConfig.ACKS_CONFIG)).isEqualTo("all");
        assertThat(producer.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo(true);
        // 5 is the maximum that preserves ordering with idempotence enabled.
        assertThat(producer.get(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION)).isEqualTo(5);
    }

    @Test
    @DisplayName("producer timeouts are explicit so experiments are reproducible")
    void producerTimeoutsAreExplicit() {
        assertThat(producer.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG))
                .isEqualTo(KafkaContract.DELIVERY_TIMEOUT_MS);
        assertThat(producer.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG))
                .isEqualTo(KafkaContract.REQUEST_TIMEOUT_MS);
        assertThat(producer.get(ProducerConfig.MAX_BLOCK_MS_CONFIG))
                .isEqualTo(KafkaContract.MAX_BLOCK_MS);
        assertThat(KafkaContract.describe()).containsKeys(
                "acks", "deliveryTimeoutMs", "requestTimeoutMs", "maxBlockMs", "enableAutoCommit");
    }

    @Test
    @DisplayName("consumer never auto-commits, so offsets follow durability rather than precede it")
    void consumerContract() {
        assertThat(consumer.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)).isEqualTo(false);
        assertThat(consumer.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)).isEqualTo("earliest");
        assertThat(consumer.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG)).isEqualTo("read_committed");
    }

    @Test
    @DisplayName("typed consumer props pin the deserializer and trusted packages")
    void typedConsumerProps() {
        Map<String, Object> typed =
                KafkaContract.consumerProps("localhost:9092", "g", OrderCreatedEvent.class);
        // The value is the Class itself, not its name -- both are accepted by Spring Kafka's
        // JsonDeserializer, and passing the Class avoids a name/typo mismatch at startup.
        assertThat(typed).containsEntry(
                "spring.json.value.default.type", OrderCreatedEvent.class);
        assertThat(typed).containsEntry("spring.json.trusted.packages", "com.recoveryguard.events");
        assertThat(typed).containsEntry(
                "spring.deserializer.value.delegate.class",
                org.springframework.kafka.support.serializer.JsonDeserializer.class);
    }
}
