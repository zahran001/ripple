# Ripple — build plan + phase tracker

## Status key

- `[ ]` not started
- `[~]` in progress
- `[x]` complete

---

## Current phase: Phase 1 — Weeks 1–2

Update the line above when moving to a new phase.

---

## Phase 1 — Probe engine + circuit breaker (Weeks 1–2)

**Concepts:** Concurrency & thread safety, Fault tolerance & circuit breaking

**Milestone:** Circuit breaker correctly opens after N failures and transitions to half-open after cooldown — verified with Awaitility. ProbeScheduler fans out to 10+ mock services concurrently without data races.

### Tasks

- [ ] `ProbeScheduler` — virtual thread per probe, `Semaphore`-bounded concurrency
- [ ] `CircuitBreaker` — hand-rolled state machine, `AtomicReference<State>`, no Resilience4j
- [ ] `HttpProber` — HTTP health check with configurable timeout
- [ ] `GrpcProber` — gRPC health check protocol (grpc.health.v1)
- [ ] `TcpProber` — raw TCP connection check
- [ ] `ScriptProber` — executes a configurable shell script, reads exit code
- [ ] `ProbeResult` record — typed result with status, latency, timestamp
- [ ] `ProbeStatus.DEGRADED` — latency-threshold breach treated as a distinct status from hard failure
- [ ] `LatencyThresholdConfig` — per-service configurable p99 latency threshold (via `application.yml`)
- [ ] `ProbeScheduler` emits `DEGRADED` result when response time exceeds threshold but service is reachable
- [ ] Unit tests: `CircuitBreakerTest` with Awaitility for all state transitions
- [ ] Unit tests: `ProbeSchedulerTest` — concurrency, semaphore cap, virtual thread usage
- [ ] Testcontainers scaffold — base class for integration tests

### Notes

_Add notes here as you work through the phase._

---

## Phase 2 — Topology graph engine (Weeks 3–4)

**Concepts:** Concurrency & thread safety

**Milestone:** Topology correctly maps Online Boutique's 11 services via auto-discovery on first `docker-compose up`. No data races under concurrent registration stress test.

### Tasks

- [ ] `TopologyGraph` — `ConcurrentHashMap` adjacency list, `StampedLock` for mutations
- [ ] Optimistic read path with fallback to read lock
- [ ] `GraphSnapshot` — immutable point-in-time copy
- [ ] Topological sort + cycle detection (Kahn's algorithm) on edge add
- [ ] `DiscoveryEndpoint` — `POST /services/register` REST endpoint
- [ ] Manual registration endpoint — `DELETE /services/{id}`
- [ ] TTL-based stale entry eviction
- [ ] `TopologyDiffEmitter` — detects mutations, emits `TopologyEvent`
- [ ] Unit tests: `TopologyGraphTest` — concurrent read/write, cycle detection, snapshot correctness
- [ ] Integration test: topology auto-discovery with Online Boutique (Testcontainers + docker-compose)

### Notes

_Add notes here as you work through the phase._

---

## Phase 3 — Blast radius engine (Weeks 5–6)

**Concepts:** Concurrency & thread safety, Graceful degradation

**Milestone:** Killing `productcatalogservice` in Online Boutique simulation returns the correct affected set (`frontend`, `recommendationservice`, `checkoutservice`) within 500ms.

### Tasks

- [ ] `BlastRadiusEngine` — parallel BFS over topology DAG using `ForkJoinPool`
- [ ] `AffectedSet` — ordered map of `ServiceId → DegradationScore`
- [ ] Severity scoring — weighted by dependency depth + criticality annotation
- [ ] `FailureSimulator` — hypothetical failure on a cloned snapshot
- [ ] `CascadeSimulator` — extends `FailureSimulator`; after marking node failed, propagates failure to dependents that exceed a configurable cascade threshold (no circuit breaker open to stop it), iterating until the cascade stabilises
- [ ] `CascadeResult` record — wraps `BlastRadiusResult` with a per-service `FailureMode` (`HARD_DOWN` | `DEGRADED` | `CASCADED`) and the cascade wave each service was affected in
- [ ] `POST /simulate` extended request body — accepts optional `cascadeEnabled: true` flag and `cascadeThreshold` (fraction of dependencies that must fail before a node itself fails)
- [ ] `HistoryReplayer` — replays stored `GraphSnapshot` sequences
- [ ] Partial result mode — returns best-effort result when graph is incomplete
- [ ] `GET /blast-radius/{id}` REST endpoint
- [ ] `POST /simulate` REST endpoint (hypothetical failure body)
- [ ] Unit tests: `BlastRadiusEngineTest` — known graph topologies, expected affected sets
- [ ] Integration test: Online Boutique blast radius correctness for all 11 services

### Notes

_Add notes here as you work through the phase._

---

## Phase 4 — Failure event stream + backpressure (Weeks 7–8)

**Concepts:** Backpressure & rate limiting, Concurrency & thread safety

**Milestone:** Slow SSE subscriber is correctly shed at high-water mark while fast subscribers receive all events — zero memory leak confirmed under 10-minute load test.

### Tasks

- [ ] `EventBus` — Chronicle Queue ring buffer, producer append, per-subscriber tailer
- [ ] `Subscriber` interface — `onEvent()`, `highWaterMark()`
- [ ] `BackpressureMonitor` — lag tracking, shed events when > high-water mark
- [ ] `SUBSCRIBER_LAGGING` metric via Micrometer
- [ ] `AlertRouterSubscriber` — PagerDuty, Slack webhook, generic HTTP webhook
- [ ] `StateStoreSubscriber` — PostgreSQL snapshot persistence, Redis health state
- [ ] `SseSubscriber` — Spring WebFlux `ServerSentEvent` stream
- [ ] `DegradationPlannerSubscriber` — runbook matching, `RunbookSuggestion` emission
- [ ] Chronicle Queue persistence + replay from disk
- [ ] Unit tests: `BackpressureMonitorTest` — artificial slow subscriber, shedding verification
- [ ] Integration test: Online Boutique under load generator + backpressure verification

### Notes

_Add notes here as you work through the phase._

---

## Phase 5 — API layer, rate limiting + graceful degradation (Weeks 9–10)

**Concepts:** Backpressure & rate limiting, Graceful degradation, Fault tolerance & circuit breaking

**Milestone:** Rate limiter correctly rejects excess requests with `503 + Retry-After`. `/health` returns structured JSON covering circuit states, subscriber lag, and liveness — never a 500.

### Tasks

- [ ] Token bucket rate limiter — `AtomicLong` CAS loop, configurable per caller
- [ ] `503 + Retry-After` response on bucket exhaustion
- [ ] `/health` structured endpoint — reports `probeCircuits`, `subscriberLag`, and liveness
- [ ] `SteadyStateEvaluator` — evaluates a named set of assertions against live Micrometer metrics (probe latency p99, circuit breaker states, subscriber lag). Returns `STEADY` or `VIOLATED` with per-assertion detail
- [ ] `SteadyStateDefinition` — configuration record binding a named hypothesis to threshold values in `application.yml` (e.g. `probe.latency.p99.max-ms`, `circuit-breakers.all-closed`, `subscriber-lag.max`)
- [ ] `GET /steady-state/{name}` — evaluates the named hypothesis on demand and returns structured JSON result
- [ ] `POST /steady-state/{name}/baseline` — snapshots current metric values as the reference baseline for that hypothesis
- [ ] Unit tests: `SteadyStateEvaluatorTest` — assert STEADY/VIOLATED transitions for each assertion type
- [ ] Degradation planner — runbook registry config, `RunbookSuggestion` API response
- [ ] Prometheus metrics — circuit breaker states, subscriber lag, rate limit hits, probe latencies
- [ ] Micrometer integration with Spring Boot Actuator
- [ ] Cross-validation: Astronomy Shop blast radius vs OTel traces

### Notes

_Add notes here as you work through the phase._

---

## Phase 6 — Integration testing + portfolio polish (Weeks 11–12)

**Concepts:** All four concepts exercised end-to-end

**Milestone:** End-to-end demo recorded: kill one Online Boutique service, watch Ripple compute blast radius and cascade simulation in real time, steady-state hypothesis transitions to VIOLATED, alert router fires, dashboard updates — all on video.

### Tasks

- [ ] `docker-compose.yml` — Online Boutique + Ripple + Postgres + Redis + Grafana
- [ ] Scripted failure injection sequence (`chaos.sh` or similar)
- [ ] `chaos.sh` extended — baseline steady-state snapshot before each kill, evaluate hypothesis after kill, log STEADY/VIOLATED transitions alongside blast radius output
- [ ] Blast radius dashboard — React SSE consumer, visualises `FailureMode` per service (`HARD_DOWN` / `DEGRADED` / `CASCADED`) and cascade wave
- [ ] Grafana dashboard — circuit states, subscriber lag, probe latencies, steady-state hypothesis status panel
- [ ] Full cross-validation: Astronomy Shop predictions vs OTel trace data, including DEGRADED latency predictions vs actual trace latencies
- [ ] `README.md` — architecture decisions explained, local setup guide, demo instructions
- [ ] GitHub Actions CI — build, test, Testcontainers integration suite
- [ ] Demo recording — kill → cascade simulation → steady-state VIOLATED → alert → dashboard update

### Notes

_Add notes here as you work through the phase._

---

## Completed phases

_Move phases here with completion date when done._
