package com.recoveryguard.driver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Reproducible workload driver.
 *
 * <pre>
 * java -jar workload-driver.jar \
 *   --workload.experiment-id=exp-001 \
 *   --workload.orders=5000 \
 *   --workload.rate=50 \
 *   --workload.seed=20261001 \
 *   --workload.order-service-url=http://localhost:8081
 * </pre>
 *
 * <p>Exits non-zero if any order was rejected or left in an unknown state, so CI and experiment
 * scripts cannot accidentally treat an incomplete workload as a valid baseline.
 */
@SpringBootApplication(
        // The driver is a client of Order Service, not a Kafka participant: it must not pull in the
        // shared workload auto-configuration, which would open a ledger and declare topics it has no
        // business owning.
        scanBasePackages = "com.recoveryguard.driver")
@EnableConfigurationProperties(DriverOptions.class)
public class WorkloadDriverApplication {

    public static void main(String[] args) {
        System.exit(SpringApplication.exit(SpringApplication.run(WorkloadDriverApplication.class, args)));
    }
}
