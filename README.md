# recoveryguard-workload

Kafka-backed reference e-commerce workload used as the system under test for
[RecoveryGuard](https://github.com/KrishCodesw). This repo owns the business
services only -- Kafka/Strimzi infrastructure lives in `recoveryguard-infra`,
fault injection in `recoveryguard-failures`, and the validation engine in
`recoveryguard-core`.

## Scope: Phase 1 vs Phase 2

Only **Order -> Payment -> Inventory** is implemented. This is a deliberate
scope cut, not an oversight -- it's already a complete causal chain with a
real acknowledged-event contract, and standing up all six services before
that chain works end-to-end is how projects like this stall out.

| Service | Status | Consumes | Produces |
|---|---|---|---|
| `order-service` | **implemented** | -- | `OrderCreated` -> `orders` |
| `payment-service` | **implemented** | `orders` | `PaymentAuthorized` -> `payments` |
| `inventory-service` | **implemented** | `payments` | `InventoryReserved` -> `inventory` |
| `warehouse-service` | stub | `inventory` (Phase 2) | `WarehouseAllocated` -> `warehouse` |
| `logistics-service` | stub | `warehouse` (Phase 2) | `ShipmentCreated` -> `logistics` |
| `notification-service` | stub | `orders` (Phase 2) | -- |

## The contract that matters: what counts as "acknowledged"

Every producer in this workload runs with `acks=all`, `enable.idempotence=true`.
This is not a performance default -- it's the precondition for RecoveryGuard's
verdicts to mean anything. If you touch `KafkaConfig`/`KafkaProducerConfig` in
any service, do not relax these settings.

An order is considered **acknowledged** once it has a durable `OrderCreated`,
`PaymentAuthorized`, and `InventoryReserved` -- each individually ack'd by the
broker. After a recovery event, any order missing one of these three despite
having the others is the canonical data-integrity failure this whole project
exists to catch (e.g. payment authorized but inventory reservation silently
lost -- the system now thinks it can fulfill an order it never actually
reserved stock for).

## Running locally

```bash
docker compose up --build
```

This starts a single-broker dev Kafka (not representative of recovery
behavior -- see the warning in `docker-compose.yml`) plus the three Phase 1
services. Create an order:

```bash
curl -X POST localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-1","lines":[{"sku":"SKU-1","quantity":2,"unitPrice":19.99}]}'
```

Watch `payment-service` and `inventory-service` logs to see the event chain
fire. For actual failure/recovery experiments, use `recoveryguard-infra`'s
Strimzi cluster instead -- this compose setup has no replication and cannot
exhibit the failure modes RecoveryGuard validates against.

## Building

```bash
mvn clean package
```

Multi-module Maven reactor build (`common-events` first, then each service).

## Module layout

```
recoveryguard-workload/
├── common-events/       # shared event schemas -- framework-agnostic
├── order-service/
├── payment-service/
├── inventory-service/
├── warehouse-service/   # stub
├── logistics-service/   # stub
├── notification-service/# stub
└── docker-compose.yml   # local dev only
```
