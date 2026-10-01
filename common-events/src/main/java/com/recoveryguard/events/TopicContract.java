package com.recoveryguard.events;

import java.util.Map;

/**
 * Declarative contract for one Kafka topic in the reference workload.
 *
 * <p>Held as plain data rather than a {@code NewTopic} so that {@code common-events} stays free of
 * Spring; each service converts these into {@code NewTopic} beans via {@link #toKafkaConfigs()}.
 *
 * <p><b>Why topics must be declared explicitly.</b> The previous code relied on broker auto-create,
 * which yields one partition and a replication factor of 1. That is fatal to this project for two
 * reasons: a single-partition topic cannot be targeted partition-wise by a fault injector, and a
 * replication factor of 1 makes {@code acks=all} vacuous -- there is only ever one replica to
 * acknowledge. Neither PRD section 7.1 ("multiple topics and partitions") nor section 10.2
 * (stale replica becomes authoritative) is expressible in that configuration.
 *
 * @param name               topic name
 * @param partitions         partition count; must be &gt;= 2 for failure targeting to be meaningful
 * @param replicationFactor  replica count; must be &gt;= 2 or {@code acks=all} guarantees nothing
 * @param minInsyncReplicas  minimum ISR size for a produce to be accepted; must be &gt;= 2 or
 *                           {@code acks=all} silently degrades to {@code acks=1} when the ISR
 *                           shrinks to the leader alone
 */
public record TopicContract(String name, int partitions, int replicationFactor, int minInsyncReplicas) {

    public static final String ORDERS = "orders";
    public static final String PAYMENTS = "payments";
    public static final String INVENTORY = "inventory";

    /** Suffix applied to a topic name for its dead letter topic. */
    public static final String DLT_SUFFIX = ".DLT";

    /**
     * The Phase 1 topic set. Partition and replication defaults suit a 3-broker cluster; the
     * single-broker dev compose overrides them via {@code RECOVERYGUARD_CLUSTER_PROFILE=dev}.
     */
    public static final java.util.List<TopicContract> PHASE1 = java.util.List.of(
            new TopicContract(ORDERS, 3, 3, 2),
            new TopicContract(PAYMENTS, 3, 3, 2),
            new TopicContract(INVENTORY, 3, 3, 2)
    );

    /**
     * A relaxed profile for the single-broker local dev compose. Retention and durability guarantees
     * are <b>not</b> meaningful here; this profile exists so services start locally, and
     * {@link ClusterContractGuard} refuses to treat a dev-profile cluster as a valid experiment
     * target.
     */
    public static final java.util.List<TopicContract> DEV = java.util.List.of(
            new TopicContract(ORDERS, 1, 1, 1),
            new TopicContract(PAYMENTS, 1, 1, 1),
            new TopicContract(INVENTORY, 1, 1, 1)
    );

    /**
     * The full topic set for a profile: the three Phase 1 business topics plus their dead letter
     * topics. DLTs are declared rather than auto-created so a dropped record lands in a known,
     * inspectable place instead of vanishing -- which is what makes workload-side loss attributable
     * rather than indistinguishable from recovery-side loss.
     */
    public static java.util.List<TopicContract> forProfile(boolean devProfile) {
        java.util.List<TopicContract> base = devProfile ? DEV : PHASE1;
        java.util.List<TopicContract> all = new java.util.ArrayList<>(base);
        for (TopicContract tc : base) {
            all.add(new TopicContract(tc.dltName(), tc.partitions(),
                    devProfile ? 1 : Math.min(tc.replicationFactor(), 3),
                    devProfile ? 1 : tc.minInsyncReplicas()));
        }
        return java.util.List.copyOf(all);
    }

    public String dltName() {
        return name + DLT_SUFFIX;
    }

    /** Topic-level config entries to apply when creating the topic. */
    public Map<String, String> toKafkaConfigs() {
        return Map.of("min.insync.replicas", Integer.toString(minInsyncReplicas));
    }
}
