package com.recoveryguard.driver;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Driver configuration, bound from {@code workload.*} and overridable by CLI arguments
 * ({@code --workload.orders=5000}).
 */
@ConfigurationProperties(prefix = "workload")
public class DriverOptions {

    /** Number of orders to create. */
    private int orders = 100;

    /** Target orders per second; {@code 0} means as fast as possible. */
    private double rate = 0d;

    /**
     * Seed for the deterministic order generator. The same seed always produces the same sequence of
     * orders, which is what makes an experiment repeatable rather than merely re-runnable.
     */
    private long seed = 20260926L;

    /** Experiment identifier; every artifact this run writes is scoped to it. */
    private String experimentId = "unscoped";

    /** Base directory for this run's artifacts. */
    private String outputDir = "./runs";

    /** Order Service intake endpoint. */
    private String orderServiceUrl = "http://localhost:8081";

    /** Per-request timeout in milliseconds. */
    private long requestTimeoutMs = 180_000L;

    /**
     * Exit non-zero if any order was not acknowledged. On by default: an experiment whose workload
     * silently lost requests is not a valid experiment, and CI should refuse to treat it as one.
     */
    private boolean failOnRejection = true;

    /** Concurrency of the HTTP driver. 1 keeps the issued order strictly sequential. */
    private int concurrency = 1;

    public int getOrders() { return orders; }
    public void setOrders(int orders) { this.orders = orders; }
    public double getRate() { return rate; }
    public void setRate(double rate) { this.rate = rate; }
    public long getSeed() { return seed; }
    public void setSeed(long seed) { this.seed = seed; }
    public String getExperimentId() { return experimentId; }
    public void setExperimentId(String experimentId) { this.experimentId = experimentId; }
    public String getOutputDir() { return outputDir; }
    public void setOutputDir(String outputDir) { this.outputDir = outputDir; }
    public String getOrderServiceUrl() { return orderServiceUrl; }
    public void setOrderServiceUrl(String orderServiceUrl) { this.orderServiceUrl = orderServiceUrl; }
    public long getRequestTimeoutMs() { return requestTimeoutMs; }
    public void setRequestTimeoutMs(long requestTimeoutMs) { this.requestTimeoutMs = requestTimeoutMs; }
    public boolean isFailOnRejection() { return failOnRejection; }
    public void setFailOnRejection(boolean failOnRejection) { this.failOnRejection = failOnRejection; }
    public int getConcurrency() { return concurrency; }
    public void setConcurrency(int concurrency) { this.concurrency = concurrency; }
}
