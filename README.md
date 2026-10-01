# recoveryguard-workload

Kafka-backed reference e-commerce workload used as the system under test for
[RecoveryGuard](https://github.com/KrishCodesw). This repo owns the business services and the
workload driver only — Kafka/Strimzi infrastructure lives in `recoveryguard-infra`, fault injection
in `recoveryguard-failures`, and the validation engine in `recoveryguard-core`.

> **Status of the sibling repos.** `recoveryguard-infra`, `recoveryguard-failures` and
> `recoveryguard-core` are not created yet. Until they are, this repo is the whole project.
> `docker-compose.experiment.yml` provides a three-broker cluster as a stopgap so failure experiments
> can run before `recoveryguard-infra` exists.

## Scope: Phase 1 vs Phase 2

Only **Order → Payment → Inventory** is implemented. This is a deliberate scope cut, not an
oversight — it's already a complete causal chain with a real acknowledged-event contract, and
standing up all six services before that chain works end-to-end is how projects like this stall out.

| Service | Status | Consumes | Produces |
|---|---|---|---|
| `order-service` | **implemented** | — | `OrderCreated` → `orders` |
| `payment-service` | **implemented** | `orders` | `PaymentAuthorized` → `payments` |
| `inventory-service` | **implemented** | `payments` | `InventoryReserved` → `inventory` |
| `workload-driver` | **implemented** | — | run manifest + expected-state record |
| `warehouse-service` | stub | `inventory` (Phase 2) | `WarehouseAllocated` → `warehouse` |
| `logistics-service` | stub | `warehouse` (Phase 2) | `ShipmentCreated` → `logistics` |
| `notification-service` | stub | `orders` (Phase 2) | — |

## The contract that matters: what counts as "acknowledged"

Every producer runs with `acks=all` and `enable.idempotence=true`, defined once in
[`KafkaContract`](common-events/src/main/java/com/recoveryguard/events/KafkaContract.java) so the
three services cannot drift apart. `KafkaContractTest` fails the build if these are relaxed. This is
not a performance default — it is the precondition for RecoveryGuard's verdicts to mean anything.

An order is **acknowledged** once it has a durable `OrderCreated`, `PaymentAuthorized` and
`InventoryReserved`, each individually ack'd by the broker. After a recovery event, an order missing
one of these three despite having the others is the canonical data-integrity failure this project
exists to catch — e.g. payment authorized but inventory reservation silently lost, so the system
believes it can fulfil an order it never reserved stock for.

### `acks=all` is only as strong as `min.insync.replicas`

`acks=all` means *every replica currently in the ISR*. If the ISR shrinks to the leader alone, "all"
means one — and a record can be acknowledged to the application, then lost on failover.
`min.insync.replicas=2` converts that into a rejected produce (`NOT_ENOUGH_REPLICAS`) instead of a
silent downgrade to `acks=1`. Topics are therefore declared explicitly (RF 3, `min.insync.replicas`
2, 3 partitions) rather than left to broker auto-create, and
[`ClusterContractGuard`](common-events/src/main/java/com/recoveryguard/events/ClusterContractGuard.java)
verifies the live cluster at startup. Under `RECOVERYGUARD_CLUSTER_PROFILE=experiment` a violation
**aborts startup**; under `dev` it warns. A cluster where the contract silently doesn't hold would
produce `DATA_INTEGRITY_FAILURE` verdicts indistinguishable from real ones.

## Ground truth: the acknowledgement ledger

RecoveryGuard compares recovered state against an **independent** expectation. That expectation
cannot come from Kafka, or the check is circular — you cannot detect log loss using the log that lost
it. So every service appends to an out-of-Kafka, append-only, `fsync`'d JSONL ledger:

```
runs/<experiment-id>/ledger-order-service.jsonl
runs/<experiment-id>/ledger-payment-service.jsonl
runs/<experiment-id>/ledger-inventory-service.jsonl
```

Each row records `role`, which is what lets the validator separate genuine loss from everything else:

| Role | Meaning | Enters expected state? |
|---|---|---|
| `PRODUCED` | the broker acknowledged the write | **yes** |
| `REJECTED` | never became durable, and the caller was told so | no |
| `CONSUMED` | a listener received the record | used for causal reconstruction |
| `DROPPED` | the workload gave up and routed the record to a `.DLT` topic | no, but attributable |

Ledger writes are `fsync`'d per record by default. That is slower, and deliberate: a ledger that can
lose its tail is worse than no ledger, because it turns "never recorded" into "recorded then
vanished" — indistinguishable from the event loss the project detects.

### No silent loss inside the workload

Two mechanisms stop the workload from manufacturing the very failures it is meant to detect:

- **Durability before commit.** Chain listeners wait for the broker's answer and throw if the child
  write was not acknowledged, so the consumed offset is *not* committed and the record is redelivered.
  The previous fire-and-forget `send()` committed the parent offset before the child was durable,
  which silently dropped events during outages.
- **Dead letter topics.** Exhausted records go to `<topic>.DLT` and a `DROPPED` ledger row, instead
  of Spring's default behaviour of logging and skipping. Every workload-side loss is attributable.

The retry window is ~120s (60 × 2s) on purpose: a listener whose downstream produce fails *during an
injected outage* should ride the outage out, not dead-letter records while the cluster is recovering.

### Deterministic ids and causal links

`paymentId` and `reservationId` are derived from `orderId` via UUIDv5
([`DeterministicIds`](common-events/src/main/java/com/recoveryguard/events/DeterministicIds.java)),
and every envelope carries a `causationId` pointing at the parent event. Under at-least-once delivery
a redelivered parent produces an *identical* child, so a duplicate collapses instead of looking like
replica divergence. The UUIDv5 implementation is asserted against Python's `uuid.uuid5` reference
vectors, so a non-JVM validator can recompute the same ids independently.

## Wire format

Events are serialized by a pinned `ObjectMapper` in
[`RecoveryGuardJson`](common-events/src/main/java/com/recoveryguard/events/RecoveryGuardJson.java).
Timestamps are ISO-8601 strings with full nanosecond precision:

```json
{"envelope":{"schemaVersion":1,"eventId":"11111111-…","orderId":"order-abc",
 "eventType":"OrderCreated","producedAt":"2026-10-01T06:54:00.123456789Z"},
 "customerId":"cust-1","lines":[{"sku":"SKU-1","quantity":2,"unitPrice":19.99}],
 "totalAmount":39.98}
```

This is pinned deliberately. Spring Kafka's default mapper enables
`WRITE_DATES_AS_TIMESTAMPS`, which emits `"producedAt":1790837640.123456789` — an epoch *decimal*.
Any consumer parsing that as a JSON number receives a float64, which cannot hold 19 significant
digits, so sub-microsecond precision is silently discarded. For a measurement plan built on
detection, evidence-collection and validation latencies, lossy timestamps corrupt the recovery
timeline that justifies every verdict. `EventSchemaTest` asserts the byte-level format so an upgrade
cannot quietly change it.

Note for non-JVM consumers: records carry a `__TypeId__` header containing the Java FQCN
(`com.recoveryguard.events.OrderCreatedEvent`). Ignore it; the body is self-describing.

## Running locally

```bash
docker compose up --build
```

Single-broker dev Kafka plus the three Phase 1 services. Create an order:

```bash
curl -X POST localhost:8081/orders \
  -H "Content-Type: application/json" \
  -d '{"customerId":"cust-1","lines":[{"sku":"SKU-1","quantity":2,"unitPrice":19.99}]}'
```

The response is the acknowledgement record, and `orderId` is in it — previously it was generated
server-side and never returned, so a driver could not record what it had created:

```json
{"orderId":"d94a…","eventId":"7c1f…","eventType":"OrderCreated","topic":"orders",
 "partition":0,"offset":12,"ackedAt":"2026-10-01T06:54:00.123456789Z",
 "durable":true,"reason":null}
```

A `503` with `"durable":false` means the broker did **not** acknowledge the order. That is an explicit
negative acknowledgement, not an error to retry blindly — and such an order must never be counted as
missing after recovery, because it was never supposed to exist.

> This compose setup has no replication and **cannot** exhibit the failure modes RecoveryGuard
> validates against. The cluster-contract guard will warn at startup; that warning is correct.

## Running a real failure experiment

```bash
export RECOVERYGUARD_EXPERIMENT_ID=exp-001

# 1. three-broker cluster + services, contract guard in strict mode
docker compose -f docker-compose.experiment.yml up --build -d

# 2. generate a reproducible workload and write the ground truth
docker compose -f docker-compose.experiment.yml run --rm workload-driver \
  --workload.orders=5000 --workload.rate=50 --workload.seed=20261001

# 3. inject a broker failure and let recovery run unattended
docker compose -f docker-compose.experiment.yml stop kafka-2
sleep 60
docker compose -f docker-compose.experiment.yml start kafka-2

# 4. compare recovered Kafka state against the expectation
ls runs/exp-001/
#   manifest.json            run parameters + contract in force (FR-10)
#   expected-state.jsonl     acknowledged orders and the 3 events each must have
#   rejected.jsonl           explicit negative acknowledgements (excluded from expectation)
#   unknown.jsonl            requests with no ack decision either way (=> INCONCLUSIVE)
#   ledger-*.jsonl           per-service ground truth
```

`unknown.jsonl` matters: a request that failed at the connection level may or may not have been
written. Treating unknown as rejected hides real losses; treating it as acknowledged invents them.
A run containing unknowns should be validated as `INCONCLUSIVE`, per PRD §13.

The driver exits non-zero if any order was rejected or left unknown, so CI and experiment scripts
cannot treat an incomplete workload as a valid baseline.

## Configuration

| Property / env | Default | Purpose |
|---|---|---|
| `recoveryguard.experiment-id` / `RECOVERYGUARD_EXPERIMENT_ID` | `unscoped` | scopes every artifact this run writes |
| `recoveryguard.ledger.path` / `RECOVERYGUARD_LEDGER_PATH` | `./runs` | ledger base directory |
| `recoveryguard.ledger.fsync` / `RECOVERYGUARD_LEDGER_FSYNC` | `true` | per-record `fsync` |
| `recoveryguard.cluster.profile` / `RECOVERYGUARD_CLUSTER_PROFILE` | `dev` | `dev` warns, `experiment` aborts |
| `recoveryguard.cluster.guard-enabled` | `true` | skip the startup check in tests |
| `recoveryguard.producer.ack-timeout-ms` | `130000` | bound on waiting for the broker's answer |
| `recoveryguard.listener.retry-interval-ms` | `2000` | listener retry pacing |
| `recoveryguard.listener.max-attempts` | `60` | attempts before dead-lettering |
| `workload.orders` / `WORKLOAD_ORDERS` | `100` | driver: orders to create |
| `workload.rate` / `WORKLOAD_RATE` | `0` | driver: orders/sec, `0` = unthrottled |
| `workload.seed` / `WORKLOAD_SEED` | `20260926` | driver: deterministic generator seed |

Metrics are exported at `/actuator/prometheus` on every service (PRD §18 measurement plan).

## Building and testing

```bash
mvn clean package          # multi-module reactor build
mvn test                   # 31 tests, including an end-to-end hop on a real embedded broker
```

`PaymentChainIntegrationTest` boots a real embedded Kafka broker and the real Payment Service
context, then asserts causal linkage, deterministic ids, the ledger rows and the exact wire format.
It is the regression test for the defects that previously made this workload an unreliable system
under test.

## Module layout

```
recoveryguard-workload/
├── common-events/       # event schemas, ledger, contract, guard, ids, JSON pinning
│   └── spring/          #   auto-configuration shared by all services
├── order-service/       # HTTP intake -> OrderCreated
├── payment-service/     # OrderCreated -> PaymentAuthorized
├── inventory-service/   # PaymentAuthorized -> InventoryReserved
├── workload-driver/     # seeded generator -> manifest + expected state
├── warehouse-service/   # stub (Phase 2)
├── logistics-service/   # stub (Phase 2)
├── notification-service/# stub (Phase 2)
├── docker-compose.yml             # single-broker dev only
└── docker-compose.experiment.yml  # three-broker cluster for real experiments
```

`common-events` keeps the schemas, ledger and contract free of Spring; the optional Spring
auto-configuration lives in its own `spring` sub-package behind optional dependencies.
