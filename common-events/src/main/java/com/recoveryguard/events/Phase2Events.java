package com.recoveryguard.events;

/**
 * Placeholder event shapes for Phase 2 services (Warehouse, Logistics,
 * Notification). Not wired into any producer/consumer yet -- the services
 * in this module are stubs. Flesh these out when Phase 2 starts; keep them
 * here so the topic/event contracts are visible from day one.
 */
public final class Phase2Events {

    private Phase2Events() {
    }

    /** Emitted by Warehouse Service. Topic: {@code warehouse}. */
    public record WarehouseAllocatedEvent(EventEnvelope envelope, String warehouseId) {
    }

    /** Emitted by Logistics Service. Topic: {@code logistics}. */
    public record ShipmentCreatedEvent(EventEnvelope envelope, String shipmentId, String carrier) {
    }

    /** Emitted by Order Service once the full chain completes. Topic: {@code orders}. */
    public record OrderCompletedEvent(EventEnvelope envelope) {
    }
}
