package com.recoveryguard.logistics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Phase 2 stub: will consume WarehouseAllocated and emit ShipmentCreated.
 *
 * Deliberately not wired to Kafka yet -- Phase 1 is Order -> Payment ->
 * Inventory only (see repo README). Flesh this out when Phase 2 starts;
 * event shapes already reserved in common-events/Phase2Events.java.
 */
@SpringBootApplication
public class LogisticsServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(LogisticsServiceApplication.class, args);
    }
}
