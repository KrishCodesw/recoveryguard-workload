package com.recoveryguard.driver;

import com.recoveryguard.events.KafkaContract;
import com.recoveryguard.events.TopicContract;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Everything needed to reproduce and interpret one experiment run (PRD FR-10).
 *
 * <p>Records the workload parameters, the acknowledged-event contract in force, the topic contracts,
 * and the outcome tallies. Without this, two runs that produced different verdicts cannot be shown
 * to have had the same inputs -- which is the difference between a reproducible experiment and an
 * anecdote.
 *
 * @param schemaVersion     manifest schema version
 * @param experimentId      run identifier
 * @param seed              generator seed
 * @param requestedOrders   how many orders the driver attempted
 * @param targetRatePerSec  requested issue rate; 0 means unthrottled
 * @param concurrency       HTTP driver concurrency
 * @param orderServiceUrl   intake endpoint used
 * @param startedAt         run start
 * @param finishedAt        run end
 * @param durationMs        elapsed wall-clock
 * @param acknowledged      orders the broker confirmed durable
 * @param rejected          orders explicitly not acknowledged
 * @param transportErrors   requests that failed before an ack decision could be made
 * @param kafkaContract     the producer/consumer contract in force
 * @param topicContracts    declared topic topology
 * @param workloadVersion   Maven version of the workload
 * @param gitCommit         workload git SHA, if resolvable
 */
public record RunManifest(
        int schemaVersion,
        String experimentId,
        long seed,
        int requestedOrders,
        double targetRatePerSec,
        int concurrency,
        String orderServiceUrl,
        Instant startedAt,
        Instant finishedAt,
        long durationMs,
        int acknowledged,
        int rejected,
        int transportErrors,
        Map<String, Object> kafkaContract,
        List<Map<String, Object>> topicContracts,
        String workloadVersion,
        String gitCommit
) {
    public static final int SCHEMA_VERSION = 1;

    public static List<Map<String, Object>> describeTopics(boolean devProfile) {
        return TopicContract.forProfile(devProfile).stream()
                .<Map<String, Object>>map(tc -> Map.of(
                        "name", tc.name(),
                        "partitions", tc.partitions(),
                        "replicationFactor", tc.replicationFactor(),
                        "minInsyncReplicas", tc.minInsyncReplicas(),
                        "configs", tc.toKafkaConfigs()))
                .toList();
    }

    public static Map<String, Object> contractSnapshot() {
        return KafkaContract.describe();
    }

    /** True only if every attempted order reached a definite acknowledged/rejected outcome. */
    public boolean isComplete() {
        return transportErrors == 0
                && acknowledged + rejected == requestedOrders;
    }
}
