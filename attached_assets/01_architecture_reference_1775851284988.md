# Ripple — architecture reference

## System overview

Ripple is composed of six layers. Each layer has a single well-defined responsibility. Data flows strictly downward — upper layers produce events, lower layers consume them.

```
┌─────────────────────────────────────────────────────┐
│                  Probe engine                        │  ← fans out to N services concurrently
├────────────────────────┬────────────────────────────┤
│   Topology graph       │   Blast radius engine       │  ← graph mutation | BFS propagation
├────────────────────────┴────────────────────────────┤
│              Failure event stream                    │  ← backpressure-aware fan-out
├───────────┬────────────┬────────────┬───────────────┤
│Alert      │ State      │ SSE / WS   │ Degradation   │  ← subscribers
│router     │ store      │ feed       │ planner        │
├───────────┴────────────┴────────────┴───────────────┤
│              REST + gRPC API layer                   │  ← rate-limited ingress
└─────────────────────────────────────────────────────┘
```

---

## Layer 1 — Probe engine

**Package:** `com.ripple.engine.probe`

**Responsibility:** Continuously health-check all registered services. Emit a `ProbeResult` event for every probe attempt (success or failure).

**Key classes:**

- `ProbeScheduler` — maintains a scheduling loop per registered service. Each probe runs on a Java 21 virtual thread (`Thread.ofVirtual().start(...)`). A `Semaphore` caps the total number of concurrent in-flight probes to prevent thundering herds.
- `CircuitBreaker` — per-upstream state machine with three states: `CLOSED`, `OPEN`, `HALF_OPEN`. State transitions use `AtomicReference<State>` for lock-free correctness. When `OPEN`, the scheduler skips the probe and emits a `CIRCUIT_OPEN` result instead of attempting a connection.
- `HttpProber`, `GrpcProber`, `TcpProber`, `ScriptProber` — implementations of the `Prober` interface. Each accepts a `ServiceNode` and returns a `ProbeResult`.
- `LatencyThresholdConfig` — per-service configurable latency threshold bound from `application.yml`. When a probe succeeds but the response time exceeds the threshold, the scheduler emits `ProbeStatus.DEGRADED` rather than `SUCCESS`. This propagates into a partial blast radius — services are marked degraded rather than failed, with a reduced `DegradationScore`.

**ProbeStatus values:**

| Status | Meaning |
|---|---|
| `SUCCESS` | Reachable, within latency threshold |
| `DEGRADED` | Reachable, but response time exceeded configured threshold |
| `FAILURE` | Unreachable or returned an error |
| `CIRCUIT_OPEN` | Probe skipped — circuit breaker is open |

**Concurrency model:**

- One virtual thread per probe attempt. Threads are cheap — no pooling needed.
- The `Semaphore` is the only shared mutable state in this layer.
- `CircuitBreaker` state is `AtomicReference` — no locking.

**Circuit breaker state transitions:**

```
CLOSED ──(N consecutive failures)──► OPEN
OPEN   ──(cooldown elapsed)────────► HALF_OPEN
HALF_OPEN ──(probe succeeds)───────► CLOSED
HALF_OPEN ──(probe fails)──────────► OPEN
```

---

## Layer 2a — Topology graph engine

**Package:** `com.ripple.engine.topology`

**Responsibility:** Maintain a live, thread-safe DAG of service dependencies. Emit a `TopologyEvent` on every structural mutation (node added/removed, edge added/removed).

**Key classes:**

- `TopologyGraph` — the DAG. Backed by `ConcurrentHashMap<ServiceId, Set<ServiceId>>` for adjacency lists. Structural mutations (add/remove node or edge) acquire a write stamp via `StampedLock`. Reads (BFS traversal, snapshot) use optimistic reads with fallback to read stamp.
- `GraphSnapshot` — an immutable point-in-time copy of the topology. Used by the blast radius engine and the stale fallback.
- `DiscoveryEndpoint` — a REST endpoint (`POST /services/register`) that services call on startup to self-register with their dependencies declared.
- `TopologyDiffEmitter` — detects structural changes between consecutive snapshots and emits typed `TopologyEvent` records to the event stream.

**Locking strategy:**

```
StampedLock.tryOptimisticRead()  →  validate()  →  if invalid: readLock()
StampedLock.writeLock()          →  mutate      →  unlock
```

Optimistic reads cover the common case (topology stable during a BFS traversal). Write lock is taken only on registration/deregistration.

**Reentrancy constraint — critical rule:**

`StampedLock` is not reentrant. If a thread tries to acquire the lock while already holding it, it blocks forever waiting for itself — a deadlock. The enforced pattern is a strict two-layer split:

- **Public methods** — acquire the lock at the boundary, delegate to private helpers, release on exit. Never call another public method on the same instance.
- **Private methods** — do the actual work with no lock acquisition. Safe to call freely from within any locked public method.

```java
// Public — locks once at the boundary, delegates to private helpers
public void addEdge(ServiceId from, ServiceId to) {
    long stamp = stampedLock.writeLock();
    try {
        ensureNodeExists(from);  // private — no lock inside, safe
        ensureNodeExists(to);    // private — no lock inside, safe
        adjacency.get(from).add(to);
    } finally {
        stampedLock.unlockWrite(stamp);
    }
}

// Private — no lock acquisition, does the real work
private void ensureNodeExists(ServiceId id) {
    adjacency.putIfAbsent(id, new HashSet<>());
}
```

Any public method calling another public method on the same `TopologyGraph` instance is a deadlock. This constraint must be stated in the class-level Javadoc.

**Cycle detection:** Topological sort (Kahn's algorithm) runs on every edge addition. A cycle in the registration request is rejected with `400 CYCLE_DETECTED`.

---

## Layer 2b — Blast radius engine

**Package:** `com.ripple.engine.blast`

**Responsibility:** Given a failed (or hypothetically failed) service node, compute the set of all services that are directly or transitively affected, along with a degradation severity score for each.

**Key classes:**

- `BlastRadiusEngine` — entry point. Accepts a `ServiceId` and a `GraphSnapshot`. Returns a `BlastRadiusResult`.
- `BlastRadiusResult` — contains the `AffectedSet` (ordered by degradation severity, highest first) and metadata (computation time, snapshot timestamp).
- `AffectedSet` — a map of `ServiceId → DegradationScore`. Score is computed as a weighted function of dependency depth and the `criticality` annotation on the service node. Services with `ProbeStatus.DEGRADED` receive a reduced score rather than the maximum.
- `FailureSimulator` — marks a node as hypothetically failed on a cloned snapshot without mutating the live graph. Used for what-if analysis via the API.
- `CascadeSimulator` — extends `FailureSimulator`. After marking the root node failed, iteratively propagates failure to dependents whose fraction of failed/degraded dependencies meets a configurable `cascadeThreshold`. Repeats until no new nodes are added (the cascade has stabilised). Each affected node is tagged with its `FailureMode` (`HARD_DOWN` | `DEGRADED` | `CASCADED`) and the wave number in which it was reached.
- `CascadeResult` — wraps `BlastRadiusResult` with the per-service `FailureMode` map and wave assignments. Returned by `POST /simulate` when `cascadeEnabled: true`.
- `HistoryReplayer` — replays stored `GraphSnapshot` sequences to reconstruct and re-analyse past failure events.

**BFS algorithm:**

Parallel BFS using a `ForkJoinPool`. The initial failed node is the root. Each BFS level (direct dependents, then their dependents, etc.) is processed in parallel. Depth is tracked for severity scoring.

**Cascade algorithm:**

```
wave 0:  mark root node HARD_DOWN
wave 1:  for each node N whose dependencies contain root —
           if failedDeps(N) / totalDeps(N) >= cascadeThreshold → mark N CASCADED
wave 2:  repeat for nodes whose dependencies contain wave-1 nodes
...
repeat until no new nodes added in a wave
```

The `cascadeThreshold` defaults to `0.5` (a node cascades when half or more of its dependencies are down) and is configurable per simulation request.

---

## Layer 3 — Failure event stream

**Package:** `com.ripple.engine.stream`

**Responsibility:** Fan out `FailureEvent` records from producers (probe engine, topology engine, blast radius engine) to all registered subscribers. Apply backpressure when subscribers are slow.

**Key classes:**

- `EventBus` — the central fan-out hub. Backed by a Chronicle Queue ring buffer (off-heap, zero-GC). Producers append to the tail; each subscriber maintains its own read index (tailer).
- `Subscriber` — interface with `onEvent(FailureEvent)` and `highWaterMark()`. Each subscriber runs on its own virtual thread draining from the queue.
- `BackpressureMonitor` — tracks lag (producer index − subscriber tailer index) per subscriber. When lag exceeds the subscriber's `highWaterMark`, the monitor emits a `SUBSCRIBER_LAGGING` metric and begins shedding events for that subscriber (advances the tailer without delivering).

**Backpressure policy:**

| Subscriber lag | Action |
|---|---|
| 0 – high water mark | Normal delivery |
| > high water mark | Shed events, emit `SUBSCRIBER_LAGGING` metric |
| Subscriber disconnects | Remove tailer, stop shedding |

**Shedding does not delete events from the queue.** It advances the slow subscriber's tailer to the current producer index, skipping the backlogged events. The underlying Chronicle Queue file is untouched — other subscribers are completely unaffected.

**Per-subscriber shedding tolerance — critical design decision:**

Not all subscribers can tolerate shedding equally. Each subscriber declares its own `highWaterMark()` based on what missing an event actually means:

| Subscriber | Shedding tolerance | Why | High water mark |
|---|---|---|---|
| `AlertRouter` | Cannot shed | A missed event is a missed incident alert | Very high (effectively never shed) |
| `StateStore` | Cannot shed | Missing events means incomplete history and broken replay | Very high (effectively never shed) |
| `SseFeed` | Can shed | Dashboard recovers via snapshot fetch — see below | Low (shed early, recover fast) |
| `DegradationPlanner` | Can shed | Runbook suggestions are best-effort, not critical path | Medium |

**`SseFeed` recovery path after shedding:**

When `SseFeed` is shed, its tailer jumps to the current producer index. The dashboard client detects the sequence number gap and executes this recovery:

```
1. Detect gap — sequence number jumped forward
2. Call GET /topology — fetch the full current GraphSnapshot
3. Re-render dashboard from snapshot — accurate current state
4. Resume receiving live events from the current position
```

The client loses the step-by-step event history during the lag window, but gets a complete and accurate picture of current state. This is acceptable because a dashboard is a live view, not a historical record. History lives in the `StateStore` (PostgreSQL), which never sheds.

**Why this matters for Ripple's design:**

An observability tool that freezes its own dashboard under load — because it's buffering events for a slow browser client indefinitely — defeats its own purpose. Shedding `SseFeed` early and recovering via snapshot keeps the dashboard live and accurate even when a client is temporarily slow. The critical-path subscribers (`AlertRouter`, `StateStore`) are never affected because their tailers are independent.

---

## Layer 4 — Subscribers

### Alert router

**Package:** `com.ripple.engine.stream` (implementation in `alert` subpackage)

Consumes `FailureEvent` records and routes to configured sinks: PagerDuty, Slack webhook, or generic HTTP webhook. Retry with exponential backoff on sink failure. Dead-letter queue for events that exhaust retries. **Never sheds — high water mark set to system maximum.**

### State store

**Package:** `com.ripple.engine.stream`

Persists `GraphSnapshot` records to PostgreSQL (durable, queryable) and writes current health state to Redis (fast reads for the API layer). PostgreSQL snapshots are the source of truth for historical replay. **Never sheds — high water mark set to system maximum.**

### SSE / WebSocket feed

**Package:** `com.ripple.api`

Spring WebFlux `ServerSentEvent` stream. Each dashboard client connection is a subscriber on the event bus. Slow dashboard clients are shed after the high-water mark — they reconnect and catch up via a snapshot endpoint.

### Degradation planner

**Package:** `com.ripple.engine.stream`

Consumes blast radius results. Matches affected services against a configured runbook registry. Emits `RunbookSuggestion` records to the API layer for display on the dashboard.

---

## Layer 5 — REST + gRPC API layer

**Package:** `com.ripple.api`

**Rate limiting:** Token bucket per caller (identified by API key or IP). Implemented with `AtomicLong` CAS loop. On bucket exhaustion: `503 Service Unavailable` + `Retry-After` header. Bucket state stored in Redis for distributed deployments.

**Key endpoints:**

| Method | Path | Description |
|---|---|---|
| `POST` | `/services/register` | Register a service and its dependencies |
| `DELETE` | `/services/{id}` | Deregister a service |
| `GET` | `/topology` | Return current `GraphSnapshot` |
| `GET` | `/blast-radius/{id}` | Compute blast radius for a live service |
| `POST` | `/simulate` | Compute blast radius for a hypothetical failure; accepts `cascadeEnabled` + `cascadeThreshold` |
| `GET` | `/events` | SSE stream of `FailureEvent` records |
| `GET` | `/health` | Structured health — reports probe circuit states, subscriber lag, and liveness |
| `GET` | `/steady-state/{name}` | Evaluate a named steady-state hypothesis against live metrics; returns `STEADY` or `VIOLATED` with per-assertion detail |
| `POST` | `/steady-state/{name}/baseline` | Snapshot current metric values as the reference baseline for the named hypothesis |

---

## Steady-state hypothesis engine

**Package:** `com.ripple.engine.steadystate`

**Responsibility:** Define, baseline, and continuously evaluate named steady-state hypotheses against live Micrometer metrics. A hypothesis is a named set of threshold assertions. The engine evaluates each assertion and returns an overall `STEADY` or `VIOLATED` result with per-assertion detail.

**Key classes:**

- `SteadyStateEvaluator` — reads live metric values from the Micrometer `MeterRegistry` and evaluates them against a `SteadyStateDefinition`. Returns a `SteadyStateResult`.
- `SteadyStateDefinition` — configuration record bound from `application.yml`. Declares the named hypothesis and its assertions (e.g. probe latency p99 < 200ms, all circuit breakers closed, subscriber lag < 100 events).
- `SteadyStateResult` — record containing overall status (`STEADY` | `VIOLATED`), per-assertion outcomes, and evaluation timestamp.
- `BaselineStore` — persists a named baseline snapshot (current metric values at a point in time) to Redis. Used to compare pre/post-chaos state.

**Typical usage in chaos.sh:**

```
1. POST /steady-state/default/baseline   ← snapshot pre-chaos metrics
2. docker-compose stop productcatalogservice
3. GET  /steady-state/default            ← evaluate: expect VIOLATED
4. docker-compose start productcatalogservice
5. GET  /steady-state/default            ← evaluate: expect STEADY (after recovery)
```

**Configuration example:**

```yaml
ripple:
  steady-state:
    default:
      probe-latency-p99-max-ms: 200
      circuit-breakers-all-closed: true
      subscriber-lag-max: 100
```

---

## Data model (key records)

```java
record ServiceNode(ServiceId id, String name, Protocol protocol,
                   URI endpoint, int criticality, Duration probeInterval) {}

record ProbeResult(ServiceId serviceId, ProbeStatus status,
                   Duration latency, Instant timestamp, String detail) {}

record FailureEvent(EventType type, ServiceId origin,
                    BlastRadiusResult blastRadius, Instant timestamp) {}

record GraphSnapshot(Map<ServiceId, Set<ServiceId>> adjacency,
                     Instant capturedAt, long version) {}

record BlastRadiusResult(ServiceId failedService, AffectedSet affected,
                         Duration computedIn, Instant snapshotTimestamp) {}
```

---

## External dependencies (infrastructure)

| Dependency | Purpose | Notes |
|---|---|---|
| PostgreSQL | Durable `GraphSnapshot` storage, replay | Testcontainers in tests |
| Redis | Live health state cache, rate limit buckets | Testcontainers in tests |
| Chronicle Queue | Off-heap event ring buffer | No broker — embedded library |
| Prometheus | Metrics scraping | Via Micrometer |
| Grafana | Dashboard for circuit state, subscriber lag | docker-compose only |
