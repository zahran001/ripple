# Ripple — Coding Agent Guide

## What This Project Is

Ripple is a **failure propagation detection and monitoring system** for distributed services. It probes registered services, detects failures/degradation, computes the blast radius of failures across a dependency graph, and streams live events to a dashboard client over SSE.

Read `ARCHITECTURE.md` for the full event lifecycle and structural layer diagrams before doing any non-trivial task.

---

## Repository Layout

```
ripple/                         ← root
├── ARCHITECTURE.md             ← start here for system understanding
├── README.md                   ← API reference and setup
├── ripple/                     ← Maven project (the entire backend)
│   ├── pom.xml
│   ├── docker-compose.yml      ← PostgreSQL, Redis, Prometheus, Grafana
│   ├── Dockerfile
│   ├── infra/                  ← Grafana dashboards, Prometheus scrape config
│   └── src/
│       ├── main/java/com/ripple/
│       └── test/java/com/ripple/
```

All source lives inside `ripple/ripple/src/`. All Maven commands run from `ripple/ripple/`.

---

## Package Map

| Package | Responsibility |
|---|---|
| `com.ripple.api` | HTTP layer: controllers + rate limiting |
| `com.ripple.config` | Spring `@ConfigurationProperties` classes |
| `com.ripple.engine.blast` | Blast radius BFS, cascade + failure simulation |
| `com.ripple.engine.probe` | Probers (HTTP/gRPC/TCP/Script), CircuitBreaker, ProbeScheduler |
| `com.ripple.engine.steadystate` | Hypothesis baseline + evaluator |
| `com.ripple.engine.stream` | EventBus, RippleOrchestrator, all Subscribers, BackpressureMonitor |
| `com.ripple.engine.stream.alert` | AlertRouterSubscriber (webhook fan-out) |
| `com.ripple.engine.topology` | TopologyGraph DAG, TopologyDiffEmitter |
| `com.ripple.model` | Pure domain records/enums — no logic, no Spring |

---

## Critical Files to Read First for Each Area

**Event lifecycle (how a probe becomes an SSE event):**
→ `engine/stream/RippleOrchestrator.java` — central wiring
→ `engine/stream/EventBus.java` — Chronicle Queue publish + tailer management
→ `engine/probe/ProbeScheduler.java` — virtual thread probe loop
→ `api/SseController.java` — SSE delivery to client

**Topology and blast radius:**
→ `engine/topology/TopologyGraph.java` — StampedLock DAG, Kahn's cycle detection
→ `engine/blast/BlastRadiusEngine.java` — ForkJoin BFS, score = criticality/(depth+1)

**Backpressure and subscriber fan-out:**
→ `engine/stream/BackpressureMonitor.java` — lag = produced − consumed, shed via tailer advance
→ `engine/stream/SseSubscriber.java` — hot Flux sink, HWM=100
→ `engine/stream/StateStoreSubscriber.java` — PostgreSQL + Redis, HWM=MAX
→ `engine/stream/alert/AlertRouterSubscriber.java` — webhook retries, HWM=MAX

**Rate limiting:**
→ `api/TokenBucketRateLimiter.java` — CAS-based lazy refill, no background scheduler
→ `api/RateLimitInterceptor.java` — caller ID: X-API-Key → X-Forwarded-For → remote IP

**Circuit breaker:**
→ `engine/probe/CircuitBreaker.java` — AtomicReference FSM: CLOSED → OPEN → HALF_OPEN → CLOSED
→ `config/CircuitBreakerProperties.java` — failure-threshold, cooldown-ms

**Steady state / chaos engineering:**
→ `engine/steadystate/SteadyStateEvaluator.java` — three assertions: p99 latency, CB states, subscriber lag
→ `engine/blast/CascadeSimulator.java` — failure cascade simulation

---

## Architecture Invariants (do not violate)

1. **`com.ripple.model` has no dependencies on any other `com.ripple` package.** It is pure data. ArchUnit enforces this in `ArchitectureTest.java`.

2. **`EventBus` has a single appender, one tailer per subscriber.** Never create multiple appenders. Chronicle Queue requires this.

3. **`TopologyGraph.snapshot()` returns an immutable `GraphSnapshot`.** BFS in `BlastRadiusEngine` always operates on a snapshot — never holds the graph lock during traversal.

4. **All subscribers drain on their own virtual thread.** Never block the EventBus publish path waiting on a subscriber.

5. **`BackpressureMonitor` is the only thing that advances a tailer to the tail (shed).** Don't manually advance tailers elsewhere.

6. **`determineEventType()` in `RippleOrchestrator` returns `null` on unchanged status.** This is an intentional early exit — no event is emitted if the service status didn't change.

7. **Blast radius is only computed for `FAILURE` or `DEGRADED` statuses.** `RECOVERED` and topology events publish with `blastRadius = null`.

---

## Configuration Reference

All custom properties are in `ripple/src/main/resources/application.yml` under the `ripple:` namespace:

| Property path | Default | What it controls |
|---|---|---|
| `ripple.probe.concurrency-limit` | 50 | Semaphore cap on concurrent probe virtual threads |
| `ripple.probe.default-interval-ms` | 5000 | How often each service is probed |
| `ripple.probe.http-timeout-ms` | 2000 | HTTP probe timeout |
| `ripple.probe.default-latency-threshold-ms` | 500 | DEGRADED threshold |
| `ripple.circuit-breaker.failure-threshold` | 5 | Consecutive failures before OPEN |
| `ripple.circuit-breaker.cooldown-ms` | 30000 | OPEN → HALF_OPEN wait |
| `ripple.stream.high-water-marks.sse-feed` | 100 | SSE subscriber event shed threshold |
| `ripple.rate-limit.requests-per-second` | 100 | Sustained RPS per caller |
| `ripple.rate-limit.max-burst` | 200 | Burst capacity |
| `ripple.blast-radius.degraded-score-multiplier` | 0.5 | Score penalty for DEGRADED nodes |

Config classes: `CircuitBreakerProperties`, `ProbeProperties`, `RateLimitProperties`, `StreamProperties` in `com.ripple.config`.

---

## Running Tests

All commands run from `ripple/ripple/`:

```bash
mvn test                  # unit tests only (excludes *IT.java)
mvn verify                # unit + integration tests (needs Docker for Testcontainers)
mvn test -pl . -Dtest=ClassName   # single test class
```

Tests use:
- **JUnit 5 + AssertJ** — all test assertions
- **Mockito** — mocking (ProbeScheduler, BackpressureMonitor in SteadyStateEvaluatorTest)
- **Testcontainers** — real PostgreSQL for integration tests
- **MockWebServer** — HTTP probe tests
- **Awaitility** — async assertion waiting
- **ArchUnit** — architecture rule enforcement (`ArchitectureTest.java`)

SimpleMeterRegistry (not MockBean) is used for metric-based tests — register timers with `.publishPercentiles(0.99, 0.95, 0.50)` to match production behavior.

---

## Key Domain Concepts

**ProbeStatus** (`SUCCESS`, `DEGRADED`, `FAILURE`, `CIRCUIT_OPEN`) — the core state a service can be in after a probe attempt.

**EventType** — what `RippleOrchestrator.determineEventType()` maps status transitions to: `SERVICE_FAILURE`, `SERVICE_DEGRADED`, `SERVICE_RECOVERED`, `CIRCUIT_OPENED`, `TOPOLOGY_CHANGE`.

**BlastRadiusResult** — ordered list of affected `ServiceId`s with scores. Score = `criticality / (depth + 1)`, DEGRADED nodes multiplied by 0.5, all normalized to [0, 1].

**GraphSnapshot** — immutable deep-copy of the topology DAG taken by `TopologyGraph.snapshot()` under a read lock. BFS uses the snapshot; the live graph is never locked during traversal.

**eventIndex** — the Chronicle Queue offset stamped on every `FailureEvent` at publish time. Used as the SSE `id:` field. Client detects gaps (missed events) when `currentId > lastId + 1` and re-syncs via `GET /topology`.

---

## API Endpoints (quick reference)

| Method | Path | What it does |
|---|---|---|
| POST | `/topology/services` | Register a service node |
| POST | `/topology/edges` | Register a dependency edge |
| GET | `/topology` | Fetch full topology snapshot |
| GET | `/blast-radius/{id}` | Get precomputed blast radius for a service |
| POST | `/blast-radius/{id}/simulate` | Simulate failure (what-if) |
| POST | `/blast-radius/{id}/cascade` | Run cascade simulation |
| POST | `/steady-state/{name}/baseline` | Store a steady-state baseline |
| POST | `/steady-state/{name}/evaluate` | Evaluate current state against baseline |
| GET | `/events/stream` | SSE stream of live failure events |
| GET | `/health` | Overall system health |
| GET | `/health/circuit-breakers` | Per-service CB states |
| GET | `/health/subscribers` | Subscriber lag per subscriber |

All routes go through `RateLimitInterceptor`.

---

## Infrastructure

Local dev requires Docker:

```bash
cd ripple/ripple
docker-compose up -d     # starts PostgreSQL:5432, Redis:6379, Prometheus:9090, Grafana:3000
```

Prometheus scrapes `localhost:8080/actuator/prometheus`. Grafana dashboard is auto-provisioned from `infra/grafana/dashboards/ripple.json`.

---

## What Not to Do

- Do not add logic to `com.ripple.model` classes — they are pure data records.
- Do not call `EventBus` methods from inside a `Subscriber.onEvent()` — no re-entrant publishing.
- Do not mock `MeterRegistry` in tests that exercise metric-based assertions — use `SimpleMeterRegistry`.
- Do not create new Spring `@Scheduled` tasks without considering interaction with `BackpressureMonitor`.
- Do not use `Thread.sleep()` in production code — use virtual threads, `Awaitility` in tests.
