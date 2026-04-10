# Ripple — design decisions log

A running log of non-obvious architectural choices. Add a new entry whenever a meaningful decision is made. This keeps future conversations grounded in the reasoning that was already worked through.

Format: date · decision · context · alternatives considered · why we chose this.

---

## DDL-001 — Hand-roll the circuit breaker, no Resilience4j

**Date:** Project start

**Decision:** Implement `CircuitBreaker` from scratch using `AtomicReference<CircuitBreakerState>`.

**Context:** Resilience4j provides a production-grade circuit breaker with every feature we need. We are choosing not to use it.

**Why:** The circuit breaker is a core portfolio artefact. The value of this project to interviewers is seeing the state machine, the atomic transitions, and the half-open probing logic implemented explicitly. Depending on a library hides this entirely. A comment in code that says "this uses Resilience4j" tells an interviewer nothing about whether you understand the pattern.

**Trade-off accepted:** We lose Resilience4j's battle-tested edge case handling (thread-safety under extreme contention, metrics integration). We accept this because the probe engine's concurrency is bounded by a `Semaphore` and our failure scenarios are not extreme-contention cases.

---

## DDL-002 — Chronicle Queue over Kafka for the event ring buffer

**Date:** Project start

**Decision:** Use Chronicle Queue (embedded off-heap ring buffer) instead of Kafka for the failure event stream.

**Context:** The event stream needs to fan out `FailureEvent` records to 4 subscribers (alert router, state store, SSE feed, degradation planner) with backpressure-aware shedding.

**Why:** Chronicle Queue is an embedded library — no broker process, no ZooKeeper/KRaft, no topic management. This keeps Ripple self-contained in docker-compose. The backpressure implementation (monitoring producer index − subscriber tailer index) is explicit and visible in the codebase, which is the point. Kafka's consumer group backpressure is more sophisticated but opaque.

**Trade-off accepted:** We lose Kafka's durability guarantees, replay-at-scale, and multi-node distribution. Chronicle Queue's ring buffer is single-process. Acceptable for a project that runs on one node for portfolio purposes.

**Future:** If Ripple were to go multi-node, the event bus layer is the only thing that would change — the `Subscriber` interface is already decoupled from the transport.

---

## DDL-003 — StampedLock for the topology DAG, not synchronized

**Date:** Project start

**Decision:** Use `StampedLock` with optimistic reads for the `TopologyGraph`.

**Context:** The topology graph is read heavily (every BFS traversal, every API query) and written rarely (service registration/deregistration events).

**Why:** `StampedLock.tryOptimisticRead()` acquires no lock on the common read path — it reads a stamp, does the read, and validates the stamp. Under low write contention (our case), this eliminates lock acquisition overhead entirely on reads. `synchronized` and `ReentrantReadWriteLock` both acquire the lock even for reads.

**Trade-off accepted — reentrancy:**

`StampedLock` is not reentrant. If a thread holding the lock tries to acquire it again, it deadlocks — it blocks forever waiting for itself to release. This is different from `synchronized` and `ReentrantReadWriteLock`, which both track the owning thread and allow re-entry.

**The concrete rule this imposes:**

Every method in `TopologyGraph` must follow a strict two-layer discipline:

- **Public methods** acquire the lock exactly once at the method boundary, delegate all logic to private helpers, and release on exit. They never call another public method on the same instance.
- **Private methods** contain the actual logic and never acquire the lock. They are safe to call from within any locked public method.

The failure mode if this rule is broken:

```
addEdge() acquires write lock
  → calls hasNode()              ← hasNode() is public, tries to acquire read lock
      → StampedLock blocks       ← doesn't recognise "same thread", waits forever
      → write lock held by same thread
      → deadlock
```

This is safe in Ripple because `TopologyGraph`'s writers are three simple, non-recursive registration operations (`addNode`, `addEdge`, `removeNode`). None of them need to call back into the graph in a way that would re-acquire the lock — as long as the public/private discipline is maintained.

This constraint must be stated explicitly in `TopologyGraph`'s class-level Javadoc so any future contributor understands it immediately.

---

## DDL-004 — Partial blast radius result on incomplete graph

**Date:** Project start

**Decision:** When the topology graph is incomplete (some nodes deregistered but edges remain), the blast radius engine returns a partial result rather than an error.

**Context:** In a real service fleet, services come and go. The topology graph may have edges pointing to nodes that have deregistered. A strict implementation would refuse to compute blast radius until the graph is clean.

**Why:** Graceful degradation is a first-class goal. A partial result labelled as such is more useful than an error. The `BlastRadiusResult` carries an `isComplete` flag and a list of `unresolvedEdges`. Callers (API, dashboard) can surface this clearly.

---

## DDL-006 — Separate Online Boutique and Astronomy Shop validation roles

**Date:** Project start

**Decision:** Online Boutique is the primary test target for topology and backpressure. Astronomy Shop is used specifically for cross-validating blast radius predictions against OTel traces.

**Context:** Both could be used for all test types, but they have different strengths.

**Why:** Online Boutique has a well-documented dependency graph (easy to know the correct blast radius). Astronomy Shop has OTel instrumentation baked in — traces give ground truth for which services are actually affected by a given failure. Using each for what it's best at gives cleaner validation stories.

---

## DDL-007 — Token bucket rate limiter implemented with AtomicLong CAS, no library

**Date:** Project start

**Decision:** Implement the API rate limiter as a token bucket using an `AtomicLong` CAS loop, not Bucket4j or Guava's `RateLimiter`.

**Context:** Same portfolio rationale as DDL-001. The token bucket algorithm is a core backend engineering concept and we want the implementation to be visible.

**Why:** `AtomicLong` CAS gives us a lock-free, non-blocking implementation. The algorithm is: try to atomically subtract 1 from the bucket if > 0; if it hits 0, reject. Refill runs on a `ScheduledExecutorService` adding tokens at a configured rate.

---

---

## DDL-008 — Spring MVC + Loom over WebFlux everywhere except SSE

**Date:** Project start

**Decision:** Use Spring MVC (blocking) for all REST endpoints. Use WebFlux only for the `GET /events` SSE stream. Never use `Mono`/`Flux` outside that one endpoint.

**Context:** The initial plan listed Spring WebFlux as the web framework. Loom and WebFlux solve the same problem (high concurrency under I/O blocking) via completely different models — WebFlux via reactive non-blocking pipelines, Loom via cheap virtual threads that make blocking free. Using both is redundant and creates a mixed-paradigm codebase.

**Why:** Loom is the stated concurrency story for this project. Virtual threads let us write plain, readable blocking code everywhere — probe engine, topology graph, blast radius engine, API handlers — without callback chains or operator composition. Spring MVC with `spring.threads.virtual.enabled=true` gives us that automatically. Introducing WebFlux across the board would contradict the Loom philosophy and force reactive programming into layers where it adds no value and significant complexity.

**Why WebFlux for SSE:** Spring MVC's `SseEmitter` requires manual lifecycle management (registering the emitter, handling timeouts, cleaning up on client disconnect). WebFlux's `Flux<ServerSentEvent>` handles all of this declaratively in a handful of lines. The SSE endpoint is the one place where the reactive model genuinely pays for itself.

**Rule:** If you find yourself writing `Mono` or `Flux` anywhere outside `com.ripple.api.SseController`, stop and reconsider. The answer is almost certainly a virtual thread and a blocking call.

---

## DDL-009 — Per-subscriber shedding tolerance, not uniform backpressure

**Date:** Project start

**Decision:** Each subscriber declares its own `highWaterMark()`. `AlertRouter` and `StateStore` effectively never shed. `SseFeed` sheds early with a snapshot-based recovery path. `DegradationPlanner` sheds at a medium threshold.

**Context:** The backpressure monitor tracks lag per subscriber and sheds events when lag exceeds the high water mark. The naive approach would apply one global high water mark to all subscribers equally.

**Why uniform shedding is wrong:**

Shedding means the subscriber misses events permanently. What "missing an event" means is completely different per subscriber:

- `AlertRouter` — a missed event is a missed incident alert. An on-call engineer doesn't get paged. This is an unacceptable failure.
- `StateStore` — a missed event is a gap in the historical record. Replay and audit are broken. Also unacceptable.
- `SseFeed` — a missed event means the dashboard briefly shows stale state. Recoverable in under a second via snapshot fetch. Acceptable.
- `DegradationPlanner` — a missed event means a runbook suggestion isn't generated. Best-effort, not critical path. Acceptable.

**The recovery path is what makes SseFeed shedding safe:**

When `SseFeed` is shed, the dashboard client detects the sequence number gap, calls `GET /topology` to fetch the current full snapshot, re-renders from that snapshot, then resumes live events. The client loses the event-by-event journey but gets accurate current state — which is all a dashboard needs.

This recovery path does not exist for `AlertRouter` or `StateStore`. There is no way to retroactively fire an alert for a missed event. There is no way to reconstruct a history gap. So they must never shed.

**Implementation note:**

`highWaterMark()` is declared on the `Subscriber` interface and returned as a `long`. Each implementation returns its own value. The `BackpressureMonitor` reads it per-subscriber — no global config, no shared constant.

_Add new entries below as decisions are made during development._

---

## DDL-010 — Latency injection as a first-class probe status, not a boolean

**Date:** Post-initial-design

**Decision:** Add `ProbeStatus.DEGRADED` as a distinct status emitted when a probe succeeds but exceeds a configurable latency threshold. Propagate this into the blast radius engine as a partial degradation score rather than treating it identically to `FAILURE`.

**Context:** The original probe engine emitted only `SUCCESS` or `FAILURE`. Real-world degradation is more often slow than down — a service that responds in 4 seconds when p99 is 200ms is meaningfully impaired even if technically reachable.

**Why:** Latency degradation is the most common failure mode in distributed systems. Modelling it as distinct from hard failure makes blast radius predictions significantly more realistic. It also lets the cascade simulator distinguish between a node that is `HARD_DOWN` (causes full cascade) and one that is `DEGRADED` (causes partial cascade, depending on `cascadeThreshold`). Without this distinction, the simulator would either over-predict (treat slow as dead) or under-predict (treat slow as healthy).

**Trade-off accepted:** More states in `ProbeStatus` means more cases in the blast radius scoring logic and more test coverage needed. Accepted because the accuracy improvement is material.

---

## DDL-011 — Cascading failure simulation via iterative wave propagation

**Date:** Post-initial-design

**Decision:** Extend `FailureSimulator` with `CascadeSimulator`, which iteratively propagates failure through the dependency graph in discrete waves until no new nodes are added. A node cascades when its fraction of failed/degraded dependencies meets a configurable `cascadeThreshold`.

**Context:** The original `FailureSimulator` marked one node as failed and ran BFS to find transitively reachable nodes, but did not model whether those nodes would themselves fail as a result. In reality, services with multiple dependencies often have their own failure thresholds — they degrade gracefully when one dependency is down but fail completely when several are.

**Why:** Cascading failures are the mechanism behind most major outages (a single failed service triggering a chain of dependent failures). Modelling the wave structure — which services fail in wave 1, which in wave 2, etc. — makes the simulation significantly more informative. The `cascadeThreshold` parameter makes the model configurable: a threshold of 1.0 means a node only cascades if all its dependencies fail (very resilient); 0.5 means half suffices (more realistic for most services).

**Trade-off accepted:** The cascade algorithm is iterative and terminates only when no new nodes are added in a wave. For pathological graphs (dense, high fan-in), this could run many waves. In practice, the DAG structure (no cycles by construction) guarantees termination and real service graphs are shallow (depth 3–5). Acceptable.

**Implementation note:** `CascadeSimulator` operates on a cloned `GraphSnapshot` — the live graph is never mutated. Each wave is computed in parallel using the same `ForkJoinPool` as `BlastRadiusEngine`.

---

## DDL-012 — Steady-state hypothesis as a named, configurable assertion set

**Date:** Post-initial-design

**Decision:** Implement the steady-state hypothesis pattern as a named configuration block in `application.yml`, evaluated on demand via `GET /steady-state/{name}` and baselined via `POST /steady-state/{name}/baseline`. Assertions read directly from the live Micrometer `MeterRegistry` — no separate metrics pipeline.

**Context:** Chaos Engineering as a discipline (per Netflix's original formulation) is defined around the steady-state hypothesis: before injecting a failure, you define what normal looks like; after recovery, you verify the system returned to it. Without this, chaos injection is just random breakage with no falsifiable outcome.

**Why named + configurable rather than hard-coded:** Different test scenarios have different steady-state definitions. A latency soak test has a stricter p99 threshold than a topology registration test. Binding the definition to `application.yml` makes it overridable per environment without code changes.

**Why Micrometer directly:** Ripple already exports all relevant metrics (probe latencies, circuit breaker states, subscriber lag) via Micrometer. Reading them from the `MeterRegistry` in-process is zero-latency and requires no external dependency. The alternative — querying Prometheus — would add an HTTP round-trip and a dependency on Prometheus being up, which is ironic for a tool that evaluates system health.

**Trade-off accepted:** Evaluating assertions in-process means the steady-state evaluator shares fate with the rest of Ripple. If Ripple itself is the thing that's broken, the evaluator can't tell you. Accepted — the use case is evaluating the health of the *monitored fleet*, not Ripple itself. Ripple's own health is covered by `/health`.
