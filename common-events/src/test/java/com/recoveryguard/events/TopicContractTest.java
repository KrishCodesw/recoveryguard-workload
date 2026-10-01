package com.recoveryguard.events;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TopicContractTest {

    @Test
    @DisplayName("the experiment profile demands enough replicas for acks=all to mean anything")
    void experimentProfileIsMeaningful() {
        for (TopicContract tc : TopicContract.PHASE1) {
            assertThat(tc.replicationFactor()).isGreaterThanOrEqualTo(3);
            assertThat(tc.minInsyncReplicas()).isGreaterThanOrEqualTo(2);
            assertThat(tc.partitions()).isGreaterThanOrEqualTo(2);
            // min.insync.replicas must not exceed the replication factor, or every produce is
            // rejected outright and the workload cannot run at all.
            assertThat(tc.minInsyncReplicas()).isLessThanOrEqualTo(tc.replicationFactor());
        }
    }

    @Test
    @DisplayName("forProfile includes a dead letter topic for every business topic")
    void includesDeadLetterTopics() {
        List<TopicContract> all = TopicContract.forProfile(false);
        assertThat(all).hasSize(6);
        assertThat(all).extracting(TopicContract::name)
                .contains("orders", "payments", "inventory",
                        "orders.DLT", "payments.DLT", "inventory.DLT");
        assertThat(all).extracting(TopicContract::dltName).first().isEqualTo("orders.DLT");
    }

    @Test
    @DisplayName("the dev profile is single-replica so a local compose cluster can still start")
    void devProfileIsRelaxed() {
        for (TopicContract tc : TopicContract.forProfile(true)) {
            assertThat(tc.replicationFactor()).isEqualTo(1);
            assertThat(tc.minInsyncReplicas()).isEqualTo(1);
        }
    }

    @Test
    @DisplayName("publishes min.insync.replicas as a topic-level config")
    void exposesMinInsync() {
        assertThat(TopicContract.PHASE1.get(0).toKafkaConfigs())
                .containsEntry("min.insync.replicas", "2");
    }
}
