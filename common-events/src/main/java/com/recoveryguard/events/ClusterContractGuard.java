package com.recoveryguard.events;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Fail-fast verification that the cluster a workload instance is attached to can actually honour
 * the acknowledged-event contract.
 *
 * <p><b>Why this exists.</b> {@code acks=all} means "every replica <em>currently in the ISR</em>".
 * If the ISR shrinks to the leader alone, "all" means one. {@code min.insync.replicas} is the floor
 * that converts that situation into a rejected produce ({@code NOT_ENOUGH_REPLICAS}) instead of a
 * silent downgrade to {@code acks=1} -- and its Kafka default is 1. So a workload can hand the
 * application a durable acknowledgement for a record that exists on exactly one broker and is lost
 * on the next failover.
 *
 * <p>That is the failure PRD section 10.2 describes, but arriving at it <em>by accident</em> rather
 * than by injection is what makes experiments uninterpretable: a {@code DATA_INTEGRITY_FAILURE}
 * verdict would be indistinguishable from one caused by a misconfigured cluster. This guard turns a
 * silently vacuous experiment into a loud startup failure, which is what PRD section 13 means by
 * preferring {@code INCONCLUSIVE} to an unsupported positive result.
 *
 * <p>In {@link Profile#DEV} violations are reported but not fatal, so local single-broker iteration
 * still works. In {@link Profile#EXPERIMENT} any violation aborts startup.
 */
public final class ClusterContractGuard {

    /** How strictly to treat contract violations. */
    public enum Profile {
        /** Local single-broker development. Violations are reported, not fatal. */
        DEV,
        /** A real experiment target. Any violation aborts startup. */
        EXPERIMENT
    }

    /** Observed state of one topic. */
    public record TopicState(String name, boolean exists, int partitions, int replicationFactor,
                             String minInsyncReplicas) {
    }

    /** Outcome of an inspection. */
    public record Report(Profile profile, boolean compliant, List<String> violations,
                         int brokerCount, String clusterId, Map<String, TopicState> topics,
                         Map<String, String> brokerConfig) {

        /** Violations rendered as a single human-readable block. */
        public String render() {
            StringBuilder sb = new StringBuilder();
            sb.append("cluster contract [").append(profile).append("] ")
              .append(compliant ? "SATISFIED" : "VIOLATED").append('\n');
            sb.append("  clusterId=").append(clusterId)
              .append(" brokers=").append(brokerCount).append('\n');
            topics.forEach((k, v) -> sb.append("  topic ").append(k)
                    .append(": exists=").append(v.exists())
                    .append(" partitions=").append(v.partitions())
                    .append(" rf=").append(v.replicationFactor())
                    .append(" min.insync.replicas=").append(v.minInsyncReplicas()).append('\n'));
            brokerConfig.forEach((k, v) -> sb.append("  broker ").append(k).append('=').append(v).append('\n'));
            violations.forEach(v -> sb.append("  VIOLATION: ").append(v).append('\n'));
            return sb.toString();
        }
    }

    private static final List<String> BROKER_KEYS = List.of(
            "unclean.leader.election.enable",
            "auto.create.topics.enable",
            "min.insync.replicas",
            "default.replication.factor",
            "offsets.topic.replication.factor"
    );

    private ClusterContractGuard() {
    }

    /**
     * Inspects the cluster and returns a report. Network or admin failures are returned as
     * violations rather than thrown, so a caller can decide how to react.
     */
    public static Report inspect(String bootstrapServers, Profile profile,
                                 List<TopicContract> expected, Duration timeout) {
        List<String> violations = new ArrayList<>();
        Map<String, TopicState> topics = new LinkedHashMap<>();
        Map<String, String> brokerConfig = new LinkedHashMap<>();
        int brokerCount = 0;
        String clusterId = "unknown";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("request.timeout.ms", String.valueOf((int) timeout.toMillis()));
        props.put("default.api.timeout.ms", String.valueOf((int) timeout.toMillis()));

        try (AdminClient admin = AdminClient.create(props)) {
            try {
                var describeCluster = admin.describeCluster();
                clusterId = describeCluster.clusterId().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                brokerCount = describeCluster.nodes().get(timeout.toMillis(), TimeUnit.MILLISECONDS).size();
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                restoreInterrupt(e);
                violations.add("cannot describe cluster: " + rootMessage(e));
                return new Report(profile, false, violations, brokerCount, clusterId, topics, brokerConfig);
            }

            List<String> names = expected.stream().map(TopicContract::name).toList();
            DescribeTopicsResult dtr = admin.describeTopics(names);
            for (TopicContract tc : expected) {
                try {
                    TopicDescription td = dtr.topicNameValues().get(tc.name())
                            .get(timeout.toMillis(), TimeUnit.MILLISECONDS);
                    int rf = td.partitions().isEmpty() ? 0
                            : td.partitions().get(0).replicas().size();
                    String minIsr = readTopicConfig(admin, tc.name(), timeout);
                    topics.put(tc.name(), new TopicState(tc.name(), true, td.partitions().size(), rf, minIsr));

                    if (td.partitions().size() < tc.partitions()) {
                        violations.add(String.format(
                                "topic %s has %d partition(s), contract requires >= %d",
                                tc.name(), td.partitions().size(), tc.partitions()));
                    }
                    if (rf < tc.replicationFactor()) {
                        violations.add(String.format(
                                "topic %s has replication.factor %d, contract requires >= %d "
                                        + "(acks=all is meaningless below this)",
                                tc.name(), rf, tc.replicationFactor()));
                    }
                    int effectiveMinIsr = parseIntOr(minIsr, 1);
                    if (effectiveMinIsr < tc.minInsyncReplicas()) {
                        violations.add(String.format(
                                "topic %s has min.insync.replicas=%d, contract requires >= %d "
                                        + "(acks=all silently degrades to acks=1 when the ISR shrinks)",
                                tc.name(), effectiveMinIsr, tc.minInsyncReplicas()));
                    }
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        topics.put(tc.name(), new TopicState(tc.name(), false, 0, 0, null));
                        violations.add("topic " + tc.name() + " does not exist");
                    } else {
                        violations.add("cannot describe topic " + tc.name() + ": " + rootMessage(e));
                    }
                } catch (InterruptedException | TimeoutException e) {
                    restoreInterrupt(e);
                    violations.add("timeout describing topic " + tc.name());
                }
            }

            try {
                var resource = new ConfigResource(ConfigResource.Type.BROKER, "0");
                var cfg = admin.describeConfigs(List.of(resource)).all()
                        .get(timeout.toMillis(), TimeUnit.MILLISECONDS).get(resource);
                for (String key : BROKER_KEYS) {
                    ConfigEntry entry = cfg.get(key);
                    if (entry != null) {
                        brokerConfig.put(key, entry.value());
                    }
                }
                if (profile == Profile.EXPERIMENT) {
                    if ("true".equalsIgnoreCase(brokerConfig.get("unclean.leader.election.enable"))) {
                        violations.add("broker unclean.leader.election.enable=true -- an out-of-sync "
                                + "replica may become leader and acknowledged records will vanish");
                    }
                    if ("true".equalsIgnoreCase(brokerConfig.get("auto.create.topics.enable"))) {
                        violations.add("broker auto.create.topics.enable=true -- topics may be created "
                                + "with defaults that violate the contract; declare them explicitly");
                    }
                }
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                restoreInterrupt(e);
                // Broker config is often restricted by ACLs; treat as non-fatal evidence gap.
                brokerConfig.put("__error", rootMessage(e));
            }

            if (profile == Profile.EXPERIMENT && brokerCount > 0) {
                int maxRf = expected.stream().mapToInt(TopicContract::replicationFactor).max().orElse(1);
                if (brokerCount < maxRf) {
                    violations.add(String.format("cluster has %d broker(s) but the contract requires "
                            + "replication.factor %d -- a single-broker cluster cannot exhibit replica "
                            + "lag, ISR shrinkage or unclean leader election", brokerCount, maxRf));
                }
            }
        }

        return new Report(profile, violations.isEmpty(), violations, brokerCount, clusterId, topics, brokerConfig);
    }

    /**
     * Inspects and throws {@link IllegalStateException} if the contract is violated under
     * {@link Profile#EXPERIMENT}.
     */
    public static Report require(String bootstrapServers, Profile profile,
                                 List<TopicContract> expected, Duration timeout) {
        Report report = inspect(bootstrapServers, profile, expected, timeout);
        if (profile == Profile.EXPERIMENT && !report.compliant()) {
            throw new IllegalStateException(
                    "RecoveryGuard cluster contract violated; refusing to start.\n" + report.render());
        }
        return report;
    }

    private static String readTopicConfig(AdminClient admin, String topic, Duration timeout) {
        try {
            var resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            var cfg = admin.describeConfigs(List.of(resource)).all()
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS).get(resource);
            ConfigEntry entry = cfg.get("min.insync.replicas");
            return entry == null ? null : entry.value();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            restoreInterrupt(e);
            return null;
        }
    }

    private static int parseIntOr(String value, int fallback) {
        try {
            return value == null ? fallback : Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static String rootMessage(Throwable t) {
        Throwable cur = t;
        while (cur.getCause() != null && cur.getCause() != cur) {
            cur = cur.getCause();
        }
        return cur.getClass().getSimpleName() + ": " + cur.getMessage();
    }

    private static void restoreInterrupt(Throwable t) {
        if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }
}
