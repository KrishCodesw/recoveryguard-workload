package com.recoveryguard.events;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-language known-answer tests for the derived identifiers.
 *
 * <p>The vectors were generated with Python's {@code uuid.uuid5}, which is the RFC 4122 reference
 * implementation of SHA-1 name-based UUIDs. Asserting against them proves the Java implementation
 * agrees byte-for-byte with an independent one -- which matters because recoveryguard-core's
 * validator is expected to recompute these ids itself rather than trust the workload's output.
 */
class DeterministicIdsTest {

    private static final UUID DNS_NAMESPACE = UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

    @Test
    @DisplayName("matches the RFC 4122 / Python reference vector for the DNS namespace")
    void matchesReferenceVector() {
        assertThat(DeterministicIds.v5(DNS_NAMESPACE, "python.org"))
                .isEqualTo(UUID.fromString("886313e1-3b8a-5372-9b90-0c9aee199e5d"));
    }

    @Test
    @DisplayName("matches Python uuid5 for the RecoveryGuard namespace")
    void matchesRecoveryGuardNamespaceVectors() {
        assertThat(DeterministicIds.v5("payment:order-abc"))
                .isEqualTo(UUID.fromString("a7c9b14d-39a7-5295-9926-7cdbcab454ea"));
        assertThat(DeterministicIds.v5("reservation:order-abc"))
                .isEqualTo(UUID.fromString("7906700b-1fd4-5a45-9f6f-e6c4c6bf5b38"));
    }

    @Test
    @DisplayName("sets the version-5 and RFC 4122 variant bits")
    void setsVersionAndVariantBits() {
        UUID id = DeterministicIds.v5("payment:anything");
        assertThat(id.version()).isEqualTo(5);
        assertThat(id.variant()).isEqualTo(2);
    }

    @Test
    @DisplayName("is stable and collision-resistant across many order ids")
    void stableAndUnique() {
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 5000; i++) {
            String orderId = UUID.randomUUID().toString();
            String pay = DeterministicIds.paymentId(orderId);
            String res = DeterministicIds.reservationId(orderId);
            // Same input must always give the same output -- this is what makes a redelivery produce
            // an identical record instead of one that looks like divergence.
            assertThat(DeterministicIds.paymentId(orderId)).isEqualTo(pay);
            assertThat(DeterministicIds.reservationId(orderId)).isEqualTo(res);
            assertThat(pay).isNotEqualTo(res);
            assertThat(pay).startsWith("pay-");
            assertThat(res).startsWith("res-");
            seen.add(pay);
        }
        assertThat(seen).hasSize(5000);
    }
}
