# Ripple — tech stack + dependency decisions

Every choice below has a rationale. When Claude generates code, it uses these choices without needing to be told each time.

---

## Core runtime

| Choice | Why |
|---|---|
| Java 21 (LTS) | Virtual threads (Project Loom) are the load-bearing feature for the probe engine. One virtual thread per probe attempt is idiomatic — no thread pool sizing, no blocking concerns. Java 21 also brings records, sealed classes, and pattern matching which we use throughout the model layer. |
| Maven | Standard, well-understood, good IDE support. No Gradle DSL complexity for a project this size. |

---

## Web framework

| Choice | Why |
|---|---|
| Spring Boot 3.x + Spring MVC (blocking) | Spring MVC with `spring.threads.virtual.enabled=true` dispatches every request on a virtual thread automatically. This means plain blocking code — no `Mono`, no `Flux`, no operator chains — gets the same concurrency as a reactive stack. Loom makes WebFlux redundant for the REST layer. |
| Spring WebFlux — SSE endpoint only | The one exception: `GET /events` (the dashboard SSE stream). WebFlux's `Flux<ServerSentEvent>` is the cleanest primitive for pushing a continuous stream of events to browser clients. Using `SseEmitter` (Spring MVC's SSE mechanism) would require manual thread and timeout management. WebFlux is used here and nowhere else — no `Mono`/`Flux` in the probe engine, topology graph, blast radius engine, or any other layer. |

---

## Concurrency

| Concern | Choice | Rejected alternatives + reason |
|---|---|---|
| Probe fan-out | Java 21 virtual threads (`Thread.ofVirtual().start(...)`) | `ExecutorService` with fixed thread pool — virtual threads make pool sizing irrelevant. A blocking probe on a virtual thread costs ~few KB, not a full platform thread. |
| Probe concurrency cap | `Semaphore` with configurable, derived limit | Fixed arbitrary constant — the cap is derived as `ceil(service_count × interval_ms / timeout_ms)`. For 22 services at 5000ms interval / 2000ms timeout: ceil(22 × 2.5) = 55; default 50 covers both portfolio targets. `ProbeScheduler` logs the derivation on startup for traceability. |
| Topology DAG mutations | `StampedLock` with optimistic reads | `synchronized`, `ReentrantReadWriteLock` — `StampedLock` is faster under read-heavy workloads because optimistic reads avoid any lock acquisition in the common case (topology stable). |
| Circuit breaker state | `AtomicReference<CircuitBreakerState>` | `volatile` — `AtomicReference` gives CAS semantics so state transitions are race-free without locking. |
| Rate limiter | `AtomicLong` CAS loop (token bucket) | `synchronized` counter, Guava `RateLimiter` — CAS is non-blocking and demonstrates the technique directly. |
| Blast radius BFS | `ForkJoinPool` for parallel BFS levels | Single-threaded BFS — BFS levels are independent and parallelise cleanly. A 22-service graph at depth 5 benefits from parallel level processing. |

---

## Event streaming

| Choice | Why |
|---|---|
| Chronicle Queue | Off-heap ring buffer — zero GC pressure on the hot path. No broker to run (embedded library). Each subscriber gets its own tailer (read index), so fan-out is inherently non-blocking. Backpressure is implemented by monitoring the gap between producer and tailer index. Chosen over Kafka to keep Ripple self-contained (no external broker in docker-compose). |

**Rejected:** Kafka — adds operational complexity (ZooKeeper/KRaft, topic management, consumer groups) that is unnecessary for a single-node deployment and would obscure the backpressure implementation we want to demonstrate.

**Rejected:** `LinkedBlockingQueue` / `ArrayBlockingQueue` — on-heap, puts GC pressure on the hot path, and doesn't persist to disk for replay.

---

## Circuit breaker

| Choice | Why |
|---|---|
| Hand-rolled state machine | The entire point is to demonstrate mastery of the pattern. A hand-rolled circuit breaker with `AtomicReference` state transitions is a portfolio artefact. Resilience4j would give us the behaviour for free but hide the implementation. |

**Rejected:** Resilience4j, Hystrix — libraries. We implement it ourselves.

---

## State persistence

| Choice | Why |
|---|---|
| PostgreSQL | Durable storage for `GraphSnapshot` records and historical replay. SQL queries over snapshot history (e.g., "what was the topology at 14:00?") are natural. |
| Redis | Fast reads for current health state (probe results, circuit breaker states) and rate limiter bucket state in distributed deployments. |

Both are run via Testcontainers in integration tests — no mocks.

---

## Testing

| Choice | Why |
|---|---|
| JUnit 5 | Standard. |
| Awaitility | Async state machine tests. `await().atMost(2, SECONDS).until(...)` is far more reliable than `Thread.sleep()` for asserting eventual consistency in circuit breaker transitions and probe scheduling. |
| Testcontainers | Real PostgreSQL and Redis instances in integration tests. No in-process mocks for infrastructure — mocks lie about failure modes. |
| Mockito | For unit tests that need to stub collaborators (e.g., stubbing a `Prober` to throw `IOException` to trigger the circuit breaker). |
| ArchUnit | Enforces architectural rules as unit tests. Used specifically to enforce the `StampedLock` public/private reentrancy constraint on `TopologyGraph` — fails CI if any public method calls another public method on the same instance. |

---

## Observability

| Choice | Why |
|---|---|
| Micrometer | Standard metrics facade in Spring Boot. Bridges to Prometheus without coupling to it. |
| Prometheus | Scrapes Micrometer metrics. Chosen for the ecosystem (Grafana integration). |
| Grafana | Dashboard for circuit breaker states, subscriber lag, probe latencies. Ships in docker-compose for the demo. |

---

## Validation targets

| Target | Why |
|---|---|
| Google Online Boutique | 11 polyglot services (Go, Python, Java, Node, C#) over gRPC. Deep dependency chains. Ships with a load generator. Best for topology correctness and backpressure testing under sustained load. |
| OpenTelemetry Astronomy Shop | 22 services with OTel instrumentation baked in. Lets us cross-reference Ripple's blast radius predictions against real distributed traces — a uniquely powerful validation story. |

---

## Dependencies (pom.xml / build.gradle)

```
spring-boot-starter-webflux
spring-boot-starter-data-jpa
spring-boot-starter-data-redis
spring-boot-starter-actuator
micrometer-registry-prometheus
net.openhft:chronicle-queue
io.grpc:grpc-stub + grpc-netty-shaded
org.awaitility:awaitility          (test)
org.testcontainers:postgresql      (test)
org.testcontainers:redis           (test)
org.mockito:mockito-core           (test)
com.tngtech.archunit:archunit-junit5 (test)
```

Pin all versions in a `<dependencyManagement>` block or version catalogue. Do not use version ranges.
