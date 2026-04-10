# Ripple — project instructions

## What this project is

Ripple is a distributed failure blast-radius simulator written in Java 21. It continuously probes a fleet of microservices for health, maintains a live directed acyclic graph (DAG) of their dependencies, computes the blast radius of any real or hypothetical failure via parallel BFS, and fans out failure events to subscribers (dashboard, alerting, state store) using a backpressure-aware ring buffer.

The project is validated against two open-source microservice fleets: Google's Online Boutique (11 services, gRPC) and the OpenTelemetry Astronomy Shop (22 services, OTel instrumented).

## Current state

**Current phase:** Phase 1 — Probe engine + circuit breaker (Weeks 1–2)

Update this line as phases are completed.

## Language and runtime

- Java 21 (LTS). Always use Java 21 features — virtual threads (Project Loom), records, sealed classes, pattern matching where appropriate.
- Spring Boot 3.x with Spring MVC (blocking) for all REST endpoints — virtual threads handle concurrency, not reactive operators.
- Spring WebFlux used only for `GET /events` (SSE stream) where `Flux<ServerSentEvent>` is the cleanest primitive. Nowhere else.
- `spring.threads.virtual.enabled=true` in `application.yml` — makes Spring MVC dispatch on virtual threads automatically.
- Build tool: Maven or Gradle — match whatever is already in the repo.

## Non-negotiable technology choices

| Concern | Choice | Never use instead |
|---|---|---|
| REST endpoints | Spring MVC (blocking) + virtual threads | WebFlux / reactive operators — Loom makes them unnecessary |
| SSE stream only | Spring WebFlux `Flux<ServerSentEvent>` for `GET /events` | `SseEmitter` manual management, or reactive everywhere |
| Probe concurrency | Java 21 virtual threads (`Thread.ofVirtual()`) | Platform threads, ExecutorService with fixed pool |
| Topology DAG locking | `StampedLock` (optimistic reads) | `synchronized`, `ReentrantLock` for the main graph |
| Event ring buffer | Chronicle Queue (off-heap) | Kafka, RabbitMQ, or in-memory `LinkedBlockingQueue` |
| Circuit breaker | Hand-rolled state machine | Resilience4j, Hystrix, or any library circuit breaker |
| Rate limiting | Hand-rolled token bucket (AtomicLong CAS loop) | Bucket4j, Guava RateLimiter |
| Async test assertions | Awaitility | `Thread.sleep()` in tests |
| Integration tests | Testcontainers | In-process mocks for infrastructure (Postgres, Redis) |

## Package structure

```
com.ripple
├── engine
│   ├── probe          # ProbeScheduler, CircuitBreaker, prober implementations
│   ├── topology       # TopologyGraph, GraphSnapshot, auto-discovery
│   ├── blast          # BlastRadiusEngine, AffectedSet, severity scoring
│   └── stream         # EventBus, Subscriber, backpressure logic
├── api                # REST controllers, gRPC service definitions
├── quorum             # QuorumGuard, stale topology fallback
├── model              # Domain records: ServiceNode, ProbeResult, FailureEvent, etc.
└── config             # Spring configuration, property binding
```

New code must fit this structure. Do not propose reorganising packages mid-project.

## Code quality expectations

- All public APIs have Javadoc.
- Concurrency invariants are documented in the class-level Javadoc (e.g., "guarded by stampedLock", "thread-safe via CAS").
- No raw `Thread.sleep()` anywhere except test utilities.
- Exceptions are typed — no swallowing with empty catch blocks. Propagate or wrap with context.
- Every concurrent state transition is tested with Awaitility, not with sleep-and-check.
- Production code has zero hard-coded timeouts — all configurable via `application.yml`.

## How to respond to requests in this project

- Always fit new code into the existing package structure and conventions described above.
- When generating a new class, include the full file with package declaration, imports, and class-level Javadoc.
- When asked to implement a component, also generate its unit test class.
- Never suggest switching away from the technology choices listed above.
- If a request is ambiguous, ask one clarifying question before generating code.
- If a request would require changes across multiple files, list the affected files first before generating any code.
- Prefer showing real, runnable code over pseudocode. If a snippet is incomplete, say so explicitly.
