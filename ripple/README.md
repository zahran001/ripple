# Ripple — Distributed Failure Blast-Radius Simulator

> A production-ready Java 21 Spring Boot application that simulates how infrastructure
> failures propagate across service dependency graphs, measures their blast radius, and
> applies chaos-engineering steady-state evaluation.

---

## What This Demonstrates

| Concern | Implementation |
|---|---|
| **Java concurrency** | Virtual threads (`Thread.ofVirtual`), `StampedLock` optimistic reads, `AtomicReference` CAS, `ForkJoinPool` parallel BFS, `Semaphore` backpressure |
| **Circuit breaking** | `AtomicReference<CircuitBreakerState>` FSM — CLOSED → OPEN → HALF_OPEN with configurable failure-rate threshold |
| **Off-heap event streaming** | Chronicle Queue ring buffer with monotonic `eventIndex`, per-subscriber cursors, lag-based shedding |
| **Topology as a DAG** | `StampedLock` + Kahn's algorithm cycle detection; optimistic reads under stable topology |
| **Blast radius** | Parallel BFS via `ForkJoinPool.commonPool()`, scoring = `criticality / (depth + 1)`, normalized to [0,1] |
| **Chaos engineering** | Steady-state hypothesis evaluation — baseline capture → perturbation → deviation check |
| **SSE / reactive** | `reactor.core.publisher.Sinks.Many` multicast, client-side gap detection via `eventIndex` |
| **Rate limiting** | Token-bucket (`maxBurst` cap, lazy nanosecond refill) per remote IP |
| **Metrics** | Micrometer → Prometheus → Grafana: probe latency P99, circuit states, subscriber lag |
| **Architecture enforcement** | ArchUnit — strict downward layering, StampedLock reentrancy constraint, no cross-package leaks |

---

## Architecture

```
REST Clients
    │
    ├─ DiscoveryController   (POST /topology/services, /edges)
    ├─ BlastRadiusController (GET /blast-radius/{id}, POST /simulate, /cascade)
    ├─ SseController         (GET /events/stream)
    ├─ SteadyStateController (POST /steady-state/{name}/baseline, /evaluate)
    └─ HealthController      (GET /health)
          │
          ▼
    RippleOrchestrator  ←── ProbeScheduler (virtual thread per probe × Semaphore cap)
          │                     │
          │           CircuitBreaker (AtomicReference FSM)
          │
          ├─── TopologyGraph (StampedLock + Kahn's DAG)
          ├─── BlastRadiusEngine (ForkJoin parallel BFS)
          │
          └─── EventBus (Chronicle Queue ring buffer)
                    │
                    ├─ AlertRouterSubscriber   (PagerDuty / webhook fan-out)
                    ├─ StateStoreSubscriber    (PostgreSQL + Redis)
                    ├─ SseSubscriber           (reactor Sinks → client SSE)
                    └─ DegradationPlannerSubscriber (mitigation suggestions)
```

**Layer rule (enforced by ArchUnit):** `probe → topology → blast → stream → api`.
No layer may import from a layer above it.

---

## Quick Start

### With Docker Compose (recommended)

```bash
# Build the JAR
mvn package -DskipTests

# Start everything: app + PostgreSQL + Redis + Prometheus + Grafana
docker-compose up --build

# Application: http://localhost:8080
# Prometheus:  http://localhost:9090
# Grafana:     http://localhost:3000  (admin / ripple)
```

### Without Docker (development)

```bash
# Prerequisites: Java 21, PostgreSQL, Redis

export SPRING_DATASOURCE_URL=jdbc:postgresql://localhost:5432/ripple
export SPRING_DATASOURCE_USERNAME=ripple
export SPRING_DATASOURCE_PASSWORD=ripple
export SPRING_DATA_REDIS_HOST=localhost

mvn spring-boot:run
```

---

## Running the Chaos Demo

```bash
chmod +x chaos.sh

# Full demo: registers a 4-service chain, runs blast radius + cascade simulation
./chaos.sh demo

# Step by step:
./chaos.sh register database       http://db:5432       10
./chaos.sh register productcatalog http://api:8081      9
./chaos.sh edge productcatalog database
./chaos.sh blast   database            # who does 'database' affect?
./chaos.sh cascade database 0.5        # what cascades if >50% dependents fail?
./chaos.sh baseline my-experiment      # capture steady-state metrics
./chaos.sh evaluate my-experiment      # check if system is still in steady state
./chaos.sh events                      # stream live SSE events
```

---

## REST API

### Service Topology

```
POST   /topology/services           Register a service for health probing
GET    /topology/services           List all registered services
GET    /topology                    Full topology snapshot (JSON graph)
POST   /topology/edges              Add a dependency edge
DELETE /topology/services/{id}      Deregister a service
```

### Blast Radius

```
GET    /blast-radius/{id}           Compute current blast radius (uses live probe state)
POST   /blast-radius/{id}/simulate  Hypothetical failure (ignores current state)
POST   /blast-radius/{id}/cascade   Cascade propagation simulation
```

### Steady-State Chaos

```
POST   /steady-state/{name}/baseline   Capture current metrics as baseline
POST   /steady-state/{name}/evaluate   Evaluate hypothesis against current state
GET    /steady-state/{name}/history    Evaluation history
```

### Events

```
GET    /events/stream     SSE stream (text/event-stream); id: field = eventIndex
GET    /events/stats      Subscriber lag, shedding counters
```

### Health

```
GET    /health            Probe engine, topology, and event bus health summary
GET    /actuator/health   Spring Actuator health
GET    /actuator/prometheus   Prometheus metrics scrape endpoint
```

---

## Testing

```bash
# Unit tests only (no Spring context, no DB required)
mvn test

# Integration tests (requires Docker for Testcontainers)
mvn test -P integration

# Compile only
mvn compile
```

**Test coverage summary:**

| Class | Tests |
|---|---|
| `CircuitBreakerTest` | FSM transitions, concurrent CAS, half-open recovery |
| `ProbeSchedulerTest` | Virtual thread scheduling, exception→FAILURE, circuit interlock, deregister |
| `TopologyGraphTest` | Add/remove nodes, cycle detection (direct + transitive), DAG validation, concurrent writes |
| `BlastRadiusEngineTest` | Linear chain, fan-out, diamond DAG, scoring normalization |
| `BackpressureMonitorTest` | Slow subscriber shedding, fast subscriber unaffected, lag watermarks |
| `TokenBucketRateLimiterTest` | Burst acceptance, reject at limit, refill after interval |
| `SteadyStateEvaluatorTest` | Baseline capture, threshold breach, composite hypothesis |
| `ArchitectureTest` | Layer dependency rules, StampedLock reentrancy, cycle detection |

---

## Configuration

`src/main/resources/application.yml`:

```yaml
ripple:
  probe:
    concurrency-limit: 200      # max concurrent in-flight probes (Semaphore cap)
    default-interval-ms: 5000   # default probe interval
    connect-timeout-ms: 3000
    read-timeout-ms: 5000

  circuit-breaker:
    failure-threshold: 0.5      # open when 50%+ of a rolling window fails
    window-size: 10
    half-open-max-calls: 3
    open-duration-seconds: 30

  stream:
    data-dir: /tmp/ripple-chronicle   # Chronicle Queue storage location

  rate-limit:
    requests-per-second: 50
    max-burst: 100
```

---

## Key Design Decisions

### StampedLock — Not ReentrantReadWriteLock
`StampedLock.tryOptimisticRead()` requires no lock acquisition under stable topology
(the common case). `ReentrantReadWriteLock` always acquires on every read.
**Reentrancy constraint:** public methods lock once at the boundary and delegate to
private helpers. Private helpers never acquire the lock. Enforced by ArchUnit.

### Chronicle Queue — Not Kafka
Chronicle Queue is an off-heap, zero-GC ring buffer. At the scale of a single-host
simulator, it gives sub-microsecond append latency with no broker coordination.
The `eventIndex` field (from `appender.lastIndexAppended()`) enables SSE clients
to detect gaps and request the full topology snapshot on reconnect.

### ForkJoin for Blast Radius — Not a Thread Pool
`ForkJoinPool.commonPool()` work-steals across BFS frontier expansion.
For a wide-fan-out topology, this saturates available CPUs without over-subscribing.
Scoring formula: `criticality / (depth + 1)`, normalized to `[0, 1]`.
DEGRADED services apply a `0.5` multiplier (they contribute blast radius but less than
fully failed services).

### Virtual Threads — Not a Fixed Thread Pool
One virtual thread per probe attempt. With a `Semaphore` cap, the scheduler never
creates more than `concurrencyLimit` concurrent in-flight probes. Virtual threads
block cheaply on I/O — each probe doing an HTTP call parks rather than burning a
platform thread.

---

## Project Structure

```
ripple/
├── src/main/java/com/ripple/
│   ├── RippleApplication.java
│   ├── api/                         # REST controllers + rate limiter interceptor
│   ├── config/                      # @ConfigurationProperties + RippleConfiguration
│   ├── engine/
│   │   ├── blast/                   # BlastRadiusEngine, FailureSimulator, CascadeSimulator
│   │   ├── probe/                   # CircuitBreaker, ProbeScheduler, Prober implementations
│   │   ├── steadystate/             # SteadyStateEvaluator, BaselineStore
│   │   ├── stream/                  # EventBus, Subscriber, all subscribers
│   │   └── topology/                # TopologyGraph, TopologyDiffEmitter
│   └── model/                       # Domain records: ServiceNode, FailureEvent, etc.
├── src/test/java/com/ripple/        # Unit tests + ArchUnit
├── infra/
│   ├── prometheus/prometheus.yml
│   └── grafana/
├── docker-compose.yml
├── Dockerfile
└── chaos.sh
```
