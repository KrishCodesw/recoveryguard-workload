package com.recoveryguard.events;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 * Deterministic, name-based identifiers for derived business entities.
 *
 * <p><b>Why not {@code UUID.randomUUID()}.</b> Under at-least-once consumption a redelivered parent
 * event causes the child handler to run twice. If the child mints a random id each time, the two
 * resulting records are indistinguishable from genuine replica divergence -- and RecoveryGuard's
 * "unexpected event or divergence detection" dimension then has to guess. Deriving the id from the
 * business key makes a redelivery produce an <em>identical</em> record, so duplicates collapse
 * instead of looking like corruption.
 *
 * <p>Uses UUID version 5 (SHA-1, RFC 4122 name-based) so ids are stable across JVMs, languages and
 * runs -- a Python validator can recompute the same id independently.
 */
public final class DeterministicIds {

    /**
     * Fixed namespace for all RecoveryGuard derived identifiers. Chosen once and frozen; changing
     * it would invalidate every previously recorded id.
     */
    public static final UUID NAMESPACE =
            UUID.fromString("6f1d4a3e-9c2b-5d47-8a1e-2b3c4d5e6f70");

    private DeterministicIds() {
    }

    /** Derives the payment identifier for an order. */
    public static String paymentId(String orderId) {
        return "pay-" + v5("payment:" + orderId);
    }

    /** Derives the inventory reservation identifier for an order. */
    public static String reservationId(String orderId) {
        return "res-" + v5("reservation:" + orderId);
    }

    /**
     * Computes a UUIDv5 (SHA-1 name-based) for {@code name} under {@link #NAMESPACE}.
     */
    public static UUID v5(String name) {
        return v5(NAMESPACE, name);
    }

    /** Computes a UUIDv5 (SHA-1 name-based) for {@code name} under an explicit namespace. */
    public static UUID v5(UUID namespace, String name) {
        MessageDigest sha1;
        try {
            sha1 = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            // SHA-1 is mandated by the JCA specification; this cannot happen on a conforming JVM.
            throw new IllegalStateException("SHA-1 unavailable", e);
        }
        sha1.update(toBytes(namespace));
        sha1.update(name.getBytes(StandardCharsets.UTF_8));
        byte[] digest = sha1.digest();

        // RFC 4122 section 4.3: set the four high bits of the time_hi_and_version field to 0101
        // (version 5) and the two high bits of clock_seq_hi_and_reserved to 10 (the RFC variant).
        digest[6] &= 0x0f;
        digest[6] |= 0x50;
        digest[8] &= 0x3f;
        digest[8] |= 0x80;

        long msb = 0;
        long lsb = 0;
        for (int i = 0; i < 8; i++) {
            msb = (msb << 8) | (digest[i] & 0xffL);
        }
        for (int i = 8; i < 16; i++) {
            lsb = (lsb << 8) | (digest[i] & 0xffL);
        }
        return new UUID(msb, lsb);
    }

    private static byte[] toBytes(UUID uuid) {
        byte[] out = new byte[16];
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();
        for (int i = 0; i < 8; i++) {
            out[i] = (byte) (msb >>> (8 * (7 - i)));
            out[8 + i] = (byte) (lsb >>> (8 * (7 - i)));
        }
        return out;
    }
}
