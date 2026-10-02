package com.recoveryguard.events.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration surface shared by every workload service, bound from the {@code recoveryguard.*}
 * prefix.
 *
 * <p>Bound once here rather than re-declared per service so the three services cannot drift on
 * experiment identity, ledger durability, or cluster profile.
 */
@ConfigurationProperties(prefix = "recoveryguard")
public class RecoveryGuardProperties {

    /** Identifier for this experiment run; scopes everything the workload records. */
    private String experimentId = "";

    private final Ledger ledger = new Ledger();
    private final Cluster cluster = new Cluster();
    private final Producer producer = new Producer();
    private final Listener listener = new Listener();

    /** Topic name overrides, keyed by logical name (orders, payments, inventory). */
    private Map<String, String> topics = new HashMap<>();

    public static class Ledger {
        /** Base directory; the experiment id is appended as a subdirectory. */
        private String path = "./runs";
        /**
         * Whether each record is {@code fsync}'d before the call returns. Slower, but a ledger that
         * can lose its tail is worse than no ledger: it turns "never recorded" into "recorded then
         * vanished", which is indistinguishable from the event loss the project detects.
         */
        private boolean fsync = true;

        public String getPath() { return path; }
        public void setPath(String path) { this.path = path; }
        public boolean isFsync() { return fsync; }
        public void setFsync(boolean fsync) { this.fsync = fsync; }
    }

    public static class Cluster {
        /** {@code dev} warns on contract violations; {@code experiment} aborts startup. */
        private String profile = "dev";
        private long guardTimeoutMs = 10_000L;
        /** Allows tests and offline tooling to skip the startup cluster inspection. */
        private boolean guardEnabled = true;

        public String getProfile() { return profile; }
        public void setProfile(String profile) { this.profile = profile; }
        public long getGuardTimeoutMs() { return guardTimeoutMs; }
        public void setGuardTimeoutMs(long guardTimeoutMs) { this.guardTimeoutMs = guardTimeoutMs; }
        public boolean isGuardEnabled() { return guardEnabled; }
        public void setGuardEnabled(boolean guardEnabled) { this.guardEnabled = guardEnabled; }
    }

    public static class Producer {
        /**
         * Upper bound on waiting for the broker's answer before resolving {@code durable=false}.
         * Should exceed {@code KafkaContract.DELIVERY_TIMEOUT_MS} so the Kafka client's own timeout
         * fires first and produces a more specific reason.
         */
        private long ackTimeoutMs = 130_000L;

        public long getAckTimeoutMs() { return ackTimeoutMs; }
        public void setAckTimeoutMs(long ackTimeoutMs) { this.ackTimeoutMs = ackTimeoutMs; }
    }

    public static class Listener {
        /** Delay between retries when a downstream produce was not acknowledged. */
        private long retryIntervalMs = com.recoveryguard.events.ListenerErrorSupport.BACKOFF_INTERVAL_MS;
        /** Attempts before a record is dead-lettered. See ListenerErrorSupport for why this is large. */
        private long maxAttempts = com.recoveryguard.events.ListenerErrorSupport.MAX_ATTEMPTS;

        public long getRetryIntervalMs() { return retryIntervalMs; }
        public void setRetryIntervalMs(long retryIntervalMs) { this.retryIntervalMs = retryIntervalMs; }
        public long getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(long maxAttempts) { this.maxAttempts = maxAttempts; }
    }

    public Listener getListener() { return listener; }

    public String getExperimentId() { return experimentId; }
    public void setExperimentId(String experimentId) { this.experimentId = experimentId; }
    public Ledger getLedger() { return ledger; }
    public Cluster getCluster() { return cluster; }
    public Producer getProducer() { return producer; }
    public Map<String, String> getTopics() { return topics; }
    public void setTopics(Map<String, String> topics) { this.topics = topics; }
}
