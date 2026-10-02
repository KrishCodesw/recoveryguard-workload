package com.recoveryguard.driver;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.recoveryguard.events.RecoveryGuardJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Drives a reproducible workload against Order Service and writes the experiment's ground truth.
 *
 * <p>Produces three artifacts under {@code <outputDir>/<experimentId>/}:
 * <ul>
 *   <li>{@code expected-state.jsonl} -- one row per <b>acknowledged</b> order, listing the three
 *       events that must survive recovery. This is the independent expectation RecoveryGuard
 *       compares recovered Kafka state against.</li>
 *   <li>{@code rejected.jsonl} -- orders the broker explicitly did not acknowledge. These must be
 *       excluded from expected state: an order that was never acknowledged cannot be "missing" after
 *       recovery, and counting it would manufacture a data-integrity failure out of a rejection.</li>
 *   <li>{@code manifest.json} -- run parameters, the contract in force, and outcome tallies
 *       (PRD FR-10).</li>
 * </ul>
 *
 * <p>Requests that fail before an ack decision (connection refused, timeout) are counted separately
 * as transport errors, because they are <em>unknown</em> rather than negative: the order may or may
 * not have been written. Treating unknown as rejected would hide losses; treating it as acknowledged
 * would invent them. The manifest records them so the validator can mark the run INCONCLUSIVE.
 */
@Component
public class DriverRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(DriverRunner.class);

    /** The three events an acknowledged order must have after recovery. */
    public static final List<String> EXPECTED_CHAIN =
            List.of("OrderCreated", "PaymentAuthorized", "InventoryReserved");

    private static final ObjectMapper MAPPER = RecoveryGuardJson.mapper();

    private final DriverOptions options;
    private final ApplicationContext context;

    public DriverRunner(DriverOptions options, ApplicationContext context) {
        this.options = options;
        this.context = context;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        Path runDir = Path.of(options.getOutputDir(), options.getExperimentId());
        Files.createDirectories(runDir);
        Path expectedFile = runDir.resolve("expected-state.jsonl");
        Path rejectedFile = runDir.resolve("rejected.jsonl");
        Path unknownFile = runDir.resolve("unknown.jsonl");
        Path manifestFile = runDir.resolve("manifest.json");

        log.info("experiment {} -> {}", options.getExperimentId(), runDir.toAbsolutePath());
        log.info("orders={} rate={} seed={} target={}", options.getOrders(),
                options.getRate() == 0 ? "unthrottled" : options.getRate() + "/s",
                options.getSeed(), options.getOrderServiceUrl());

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(15))
                .build();

        OrderGenerator generator = new OrderGenerator(options.getSeed());
        AtomicInteger acknowledged = new AtomicInteger();
        AtomicInteger rejected = new AtomicInteger();
        AtomicInteger unknown = new AtomicInteger();

        Instant startedAt = Instant.now();
        long startNanos = System.nanoTime();
        // A fixed interval derived from the target rate keeps pacing independent of response time,
        // so a slow broker slows throughput rather than silently changing the issued sequence.
        long intervalNanos = options.getRate() > 0
                ? (long) (1_000_000_000d / options.getRate()) : 0L;

        try (var expected = openAppend(expectedFile);
             var rejects = openAppend(rejectedFile);
             var unknowns = openAppend(unknownFile)) {

            for (int i = 0; i < options.getOrders(); i++) {
                if (intervalNanos > 0) {
                    long sleep = startNanos + (i * intervalNanos) - System.nanoTime();
                    if (sleep > 0) {
                        Thread.sleep(sleep / 1_000_000L, (int) (sleep % 1_000_000L));
                    }
                }
                OrderGenerator.Order order = generator.next();
                submit(client, order, i, expected, rejects, unknowns,
                        acknowledged, rejected, unknown);
            }
        }

        Instant finishedAt = Instant.now();
        long durationMs = Duration.between(startedAt, finishedAt).toMillis();

        RunManifest manifest = new RunManifest(
                RunManifest.SCHEMA_VERSION,
                options.getExperimentId(),
                options.getSeed(),
                options.getOrders(),
                options.getRate(),
                options.getConcurrency(),
                options.getOrderServiceUrl(),
                startedAt, finishedAt, durationMs,
                acknowledged.get(), rejected.get(), unknown.get(),
                RunManifest.contractSnapshot(),
                RunManifest.describeTopics(false),
                resolveWorkloadVersion(),
                resolveGitCommit());

        Files.writeString(manifestFile, MAPPER.writerWithDefaultPrettyPrinter()
                .writeValueAsString(manifest), StandardCharsets.UTF_8);

        log.info("run complete in {}ms: acknowledged={} rejected={} unknown={}",
                durationMs, acknowledged.get(), rejected.get(), unknown.get());
        log.info("manifest      -> {}", manifestFile.toAbsolutePath());
        log.info("expectedState -> {}", expectedFile.toAbsolutePath());

        if (unknown.get() > 0) {
            log.warn("{} request(s) failed before an acknowledgement decision; their outcome is "
                    + "UNKNOWN, not rejected. An experiment containing unknowns should be treated as "
                    + "INCONCLUSIVE rather than clean.", unknown.get());
        }
        if (options.isFailOnRejection() && (rejected.get() > 0 || unknown.get() > 0)) {
            log.error("failing run: {} rejected, {} unknown (workload.fail-on-rejection=true)",
                    rejected.get(), unknown.get());
            System.exit(SpringApplication.exit(context, () -> 1));
        }
    }

    private void submit(HttpClient client, OrderGenerator.Order order, int index,
                        java.io.BufferedWriter expected, java.io.BufferedWriter rejects,
                        java.io.BufferedWriter unknowns,
                        AtomicInteger acknowledged, AtomicInteger rejected, AtomicInteger unknown) {
        String payload;
        try {
            payload = MAPPER.writeValueAsString(Map.of(
                    "customerId", order.customerId(),
                    "lines", order.lines().stream()
                            .map(l -> Map.of("sku", l.sku(), "quantity", l.quantity(),
                                    "unitPrice", l.unitPrice()))
                            .toList()));
        } catch (IOException e) {
            throw new IllegalStateException("cannot serialize generated order", e);
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(options.getOrderServiceUrl() + "/orders"))
                .timeout(Duration.ofMillis(options.getRequestTimeoutMs()))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        try {
            HttpResponse<String> response =
                    client.send(request, HttpResponse.BodyHandlers.ofString());
            JsonNode body = response.statusCode() == 503 || response.statusCode() == 202
                    ? safeParse(response.body()) : null;

            if (body != null && body.hasNonNull("orderId")) {
                String orderId = body.get("orderId").asText();
                boolean durable = body.path("durable").asBoolean(false);
                if (durable) {
                    acknowledged.incrementAndGet();
                    write(expected, Map.of(
                            "index", index,
                            "orderId", orderId,
                            "eventId", body.path("eventId").asText(),
                            "topic", body.path("topic").asText(),
                            "partition", body.path("partition").asInt(),
                            "offset", body.path("offset").asLong(),
                            "ackedAt", body.path("ackedAt").asText(),
                            "customerId", order.customerId(),
                            "totalAmount", order.totalAmount(),
                            "expectedChain", EXPECTED_CHAIN));
                } else {
                    rejected.incrementAndGet();
                    write(rejects, Map.of(
                            "index", index,
                            "orderId", orderId,
                            "reason", body.path("reason").asText("UNKNOWN"),
                            "httpStatus", response.statusCode()));
                }
            } else {
                // No orderId means we cannot identify the order, so its fate is unknown.
                unknown.incrementAndGet();
                write(unknowns, Map.of(
                        "index", index,
                        "httpStatus", response.statusCode(),
                        "body", truncate(response.body()),
                        "reason", "NO_ORDER_ID_IN_RESPONSE"));
            }
        } catch (IOException e) {
            // Connection-level failure: the request may or may not have been written. This is
            // UNKNOWN, deliberately not REJECTED -- conflating the two would hide real losses.
            unknown.incrementAndGet();
            write(unknowns, Map.of(
                    "index", index,
                    "reason", e.getClass().getSimpleName(),
                    "message", String.valueOf(e.getMessage())));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            unknown.incrementAndGet();
        }
    }

    private static JsonNode safeParse(String body) {
        try {
            return MAPPER.readTree(body);
        } catch (IOException e) {
            return null;
        }
    }

    private static String truncate(String s) {
        if (s == null) {
            return "";
        }
        return s.length() <= 400 ? s : s.substring(0, 400);
    }

    private static java.io.BufferedWriter openAppend(Path file) throws IOException {
        return Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /**
     * Writes one JSONL row. A serialization failure on a single row must not abort the run -- the
     * remaining orders are still valid evidence, and losing the whole run to one bad row would be a
     * worse outcome than a documented gap.
     */
    private static void write(java.io.BufferedWriter writer, Map<String, ?> row) {
        try {
            writer.write(MAPPER.writeValueAsString(row));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            log.error("failed to append a driver record; the run's ground truth is now incomplete", e);
        }
    }

    private String resolveWorkloadVersion() {
        String v = getClass().getPackage().getImplementationVersion();
        return v != null ? v : "0.1.0-SNAPSHOT";
    }

    private String resolveGitCommit() {
        String fromEnv = System.getenv("RECOVERYGUARD_GIT_COMMIT");
        return (fromEnv != null && !fromEnv.isBlank()) ? fromEnv : "unknown";
    }
}
