# Ripple

Distributed failure blast-radius simulator. Registers services as nodes in a live dependency DAG, continuously health-probes them, computes which services are affected when one fails, and propagates that failure forward through configurable cascade waves. All events stream over SSE via a Chronicle Queue ring buffer.

---

## Workspace Layout

```
ripple/                        # root — pnpm monorepo + Java backend
├── ripple/                    # Java 21 Spring Boot application  ← primary artifact
│   ├── src/main/java/com/ripple/
│   │   ├── api/               # REST controllers + rate-limit interceptor
│   │   ├── config/            # @ConfigurationProperties records
│   │   ├── engine/
│   │   │   ├── blast/         # BlastRadiusEngine, FailureSimulator, CascadeSimulator
│   │   │   ├── probe/         # CircuitBreaker, ProbeScheduler, Prober impls
│   │   │   ├── steadystate/   # SteadyStateEvaluator, BaselineStore
│   │   │   ├── stream/        # EventBus (Chronicle Queue), subscribers, BackpressureMonitor
│   │   │   └── topology/      # TopologyGraph (StampedLock DAG), TopologyDiffEmitter
│   │   └── model/             # Immutable records: ServiceNode, FailureEvent, etc.
│   ├── src/test/java/com/ripple/   # 8 JUnit 5 test suites
│   ├── docker-compose.yml     # app + postgres + redis + prometheus + grafana
│   ├── Dockerfile
│   ├── chaos.sh               # CLI for registering services and triggering simulations
│   └── pom.xml
├── artifacts/
│   ├── api-server/            # Express 5 + Drizzle Node API server
│   └── mockup-sandbox/        # React 19 + Tailwind v4 component playground (Vite)
├── lib/
│   ├── api-client-react/      # TanStack Query hooks wrapping the Java API
│   ├── api-spec/              # OpenAPI spec + Orval codegen config
│   ├── api-zod/               # Zod schemas generated from api-spec
│   └── db/                    # Drizzle ORM schema + PostgreSQL integration
└── scripts/                   # TypeScript utilities
```

The Java application in `ripple/` is the primary runtime artifact. The Node.js workspace (`artifacts/`, `lib/`) provides a React component sandbox, a generated API client, and a lightweight Express server.

---

## Technology Stack

**Backend**

| Layer | Technology |
|---|---|
| Runtime | Java 21, virtual threads (`spring.threads.virtual.enabled=true`) |
| Framework | Spring Boot 3.3.5 (MVC + WebFlux for SSE only) |
| Event queue | Chronicle Queue 2026.2 (off-heap ring buffer, zero GC) |
| Probe protocols | HTTP (JDK `HttpClient`), gRPC 1.68 Health protocol, TCP connect, Shell script |
| Metrics | Micrometer → Prometheus → Grafana |
| Persistence | PostgreSQL 16 (JPA/Hibernate), Redis 7 (state cache) |
| Build | Maven 3, `--enable-preview` for Java 21 preview features |

**Frontend / Tooling**

| Layer | Technology |
|---|---|
| UI | React 19, TypeScript 5.9, Vite 7, Tailwind CSS v4 |
| Components | Radix UI (headless), Framer Motion, Recharts |
| API client | TanStack Query + Zod + Orval codegen |
| ORM | Drizzle ORM |
| Package manager | pnpm workspace |

---

## Running

### Docker (recommended)

```bash
cd ripple
docker compose up -d
```

Services:

| Service | Port | Credentials |
|---|---|---|
| Ripple API | 8080 | — |
| PostgreSQL 16 | 5432 | ripple / ripple |
| Redis 7 | 6379 | — |
| Prometheus | 9090 | — |
| Grafana | 3000 | admin / ripple |

### Manual

```bash
# Prerequisites: Java 21, PostgreSQL, Redis
cd ripple
mvn package -DskipTests
java --enable-preview -jar target/ripple-*.jar
```

Environment variable `PORT` overrides the default port 8080.

### Chaos demo

```bash
cd ripple
chmod +x chaos.sh
./chaos.sh demo   # registers 4 services, runs blast + cascade simulation
```

Individual commands:

```bash
./chaos.sh register <id> <url> [criticality]
./chaos.sh edge <from> <to>
./chaos.sh blast <service-id>
./chaos.sh cascade <service-id> [threshold]
./chaos.sh baseline <hypothesis-name>
./chaos.sh evaluate <hypothesis-name>
./chaos.sh events          # tail SSE stream
```

---

## Architecture

```
REST Clients
    │
    ▼
RateLimitInterceptor          (token bucket, per-caller)
    │
    ├── DiscoveryController   POST/DELETE /topology/services, POST /topology/edges, GET /topology
    ├── BlastRadiusController GET /blast-radius/{id}, POST .../simulate, POST .../cascade
    ├── SteadyStateController POST .../baseline, GET .../baseline, POST .../evaluate
    ├── HealthController      GET /health, /health/circuit-breakers, /health/subscribers
    └── SseController         GET /events/stream  (text/event-stream)
            │
            ▼
    RippleOrchestrator
    ┌───────────────────────────────────────────────────┐
    │  ProbeScheduler ──────────────────────────────►  │
    │    one virtual thread per probe attempt           │
    │    Semaphore caps concurrency                     │
    │    CircuitBreaker guards each service             │
    │         │                                        │
    │         ▼  ProbeResult                           │
    │  liveProbeStatuses (ConcurrentHashMap)           │
    │         │ if FAILURE/DEGRADED                    │
    │         ▼                                        │
    │  BlastRadiusEngine.compute()                     │
    │    Parallel BFS on immutable GraphSnapshot        │
    │    ForkJoinPool, one task per BFS level          │
    │         │                                        │
    │         ▼  FailureEvent                          │
    │  EventBus (Chronicle Queue)                      │
    │    stamps eventIndex on append                   │
    │    per-subscriber named tailers                  │
    │    BackpressureMonitor sheds slow subscribers    │
    └───────────┬───────────────────────────────────────┘
                │
    ┌───────────┼────────────────────────────────────────┐
    ▼           ▼              ▼                   ▼
AlertRouter  StateStore   SseSubscriber   DegradationPlanner
(∞ HWM)     (∞ HWM)      (100 HWM)       (1000 HWM)
```

**Key concurrency properties:**
- `TopologyGraph` uses `StampedLock` with optimistic reads; structural mutations take an exclusive write lock.
- `BlastRadiusEngine` operates entirely on immutable `GraphSnapshot` — no locks held during BFS.
- `CircuitBreaker` state machine uses `AtomicReference` CAS — no locking.
- `ProbeScheduler` spawns one virtual thread per probe attempt; a `Semaphore` bounds total in-flight probes.

---

## API Reference

### Topology — `POST /topology/services`

Register a service node.

```json
{
  "id": "checkout",
  "name": "Checkout Service",
  "protocol": "HTTP",
  "endpoint": "http://checkout:8080/health",
  "criticality": 9,
  "probeIntervalMs": 5000,
  "latencyThresholdMs": 300
}
```

`protocol` ∈ `HTTP | GRPC | TCP | SCRIPT`  
`criticality` ∈ `[1, 10]` — used directly in blast radius scoring

Response:
```json
{ "id": "checkout", "topologyVersion": 3 }
```

`topologyVersion` increments on every structural mutation.

---

### Topology — `POST /topology/edges`

Declare that `from` depends on `to`. Adding an edge that would create a cycle returns HTTP 400.

```json
{ "from": "checkout", "to": "database" }
```

---

### Topology — `GET /topology`

Current topology snapshot:

```json
{
  "adjacency":        { "checkout": ["database"] },
  "reverseAdjacency": { "database": ["checkout"] },
  "nodes":            { "checkout": { ... } },
  "capturedAt":       "2026-05-25T10:00:00Z",
  "version":          3
}
```

---

### Topology — `DELETE /topology/services/{id}`

Remove a node. All edges touching it are removed.

---

### Blast Radius — `GET /blast-radius/{serviceId}`

Compute which services are affected if `serviceId` fails right now (uses live probe statuses).

```json
{
  "failedService": "database",
  "affected": {
    "scores": {
      "checkout":  { "raw": 4.5, "normalized": 1.0,  "depth": 1 },
      "frontend":  { "raw": 2.0, "normalized": 0.44, "depth": 2 }
    }
  },
  "computedIn":         "PT0.003S",
  "snapshotTimestamp":  "2026-05-25T10:00:00Z",
  "isComplete":         true,
  "unresolvedEdges":    []
}
```

**Scoring formula:**

```
rawScore  = criticality / (depth + 1)
if DEGRADED: rawScore *= 0.5
normalized = rawScore / max(rawScore across affected set)   → [0, 1]
```

---

### Blast Radius — `POST /blast-radius/{serviceId}/simulate`

Hypothetical simulation: marks `serviceId` as FAILURE regardless of live state.

Same response shape as `GET /blast-radius/{serviceId}`.

---

### Blast Radius — `POST /blast-radius/{serviceId}/cascade?cascadeThreshold=0.5`

Iterative wave cascade simulation.

```json
{
  "blastRadius": { ... },
  "failureModes": {
    "database":  "HARD_DOWN",
    "checkout":  "CASCADED",
    "frontend":  "CASCADED"
  },
  "waveAssignments": {
    "database": 0,
    "checkout": 1,
    "frontend": 2
  },
  "cascadeThreshold": 0.5,
  "totalWaves": 3
}
```

`cascadeThreshold` (0.0–1.0): a node cascades when the fraction of its failed/degraded dependencies meets or exceeds this value. Default `0.5`.

`FailureMode` ∈ `HARD_DOWN | CASCADED | DEGRADED`

---

### Steady State — `POST /steady-state/{name}/baseline`

Capture current metrics as baseline. Returns `Map<String, Double>`.

---

### Steady State — `GET /steady-state/{name}/baseline`

Retrieve stored baseline.

---

### Steady State — `POST /steady-state/{name}/evaluate`

Evaluate hypothesis against live metrics. Body is optional (uses defaults from `application.yml`):

```json
{
  "probeLatencyP99MaxMs": 500,
  "circuitBreakersAllClosed": true,
  "subscriberLagMax": 500
}
```

Response:

```json
{
  "status": "VIOLATED",
  "hypothesisName": "pre-chaos",
  "outcomes": [
    { "name": "probeLatencyP99", "passed": false, "detail": "p99=712ms > 500ms" },
    { "name": "circuitBreakersAllClosed", "passed": true, "detail": "all closed" },
    { "name": "subscriberLag", "passed": true, "detail": "max lag=42" }
  ]
}
```

`status` ∈ `STEADY | VIOLATED`

---

### Health — `GET /health`

```json
{
  "status": "DEGRADED",
  "registeredServices": 4,
  "circuitBreakers": [
    { "serviceId": "database", "state": "OPEN", "consecutiveFailures": 6 }
  ],
  "subscriberLag": [
    { "name": "sse-feed", "lag": 0 }
  ],
  "checkedAt": "2026-05-25T10:00:00Z"
}
```

`OverallStatus` ∈ `HEALTHY | DEGRADED | UNKNOWN`. Never returns HTTP 500.

Additional endpoints: `GET /health/circuit-breakers`, `GET /health/circuit-breakers/{serviceId}`, `GET /health/subscribers`.

---

### SSE — `GET /events/stream`

`Content-Type: text/event-stream`

```
id: 94489280512
event: SERVICE_FAILURE
data: {"type":"SERVICE_FAILURE","origin":"database","blastRadius":{...},"timestamp":"...","eventIndex":94489280512}
```

`id` is the Chronicle Queue append index. If the client detects `currentId > lastId + 1` (gap), it fetches `GET /topology` for a full snapshot, re-renders, then resumes.

`EventType` values: `SERVICE_FAILURE | SERVICE_DEGRADED | SERVICE_RECOVERED | CIRCUIT_OPENED | CIRCUIT_HALF_OPENED | CIRCUIT_CLOSED | NODE_ADDED | NODE_REMOVED | EDGE_ADDED | EDGE_REMOVED | NODE_EVICTED`

---

## Engines

### BlastRadiusEngine

Parallel BFS over the reverse-adjacency map using `ForkJoinPool.commonPool()`. One `ForkJoinTask` per BFS level processes all nodes in that level concurrently. A `ConcurrentHashMap` prevents revisiting nodes at greater depth. After BFS completes, scores are normalized across the full affected set.

Time: O(V + E). Space: O(V).

### CascadeSimulator

Iterative wave propagation:

```
Wave 0: root → HARD_DOWN
Wave N: for each candidate dependent of wave N-1 nodes
          fraction = (HARD_DOWN + CASCADED + DEGRADED deps) / total deps
          if fraction >= cascadeThreshold → CASCADED, enqueue for wave N+1
        stop when no new nodes added
```

Each wave is processed in parallel via ForkJoinPool. Terminates because the graph is a DAG (cycle detection on `POST /topology/edges` ensures this). Practical depth is 3–5 hops, so cascade stabilizes in 2–4 waves.

### CircuitBreaker

Hand-rolled state machine (not Resilience4j) using `AtomicReference<CircuitBreakerState>` and `AtomicInteger` — all transitions via CAS, no locking.

```
CLOSED    → [N consecutive failures]  → OPEN
OPEN      → [cooldown elapsed]        → HALF_OPEN
HALF_OPEN → [next probe succeeds]     → CLOSED
HALF_OPEN → [next probe fails]        → OPEN
```

Defaults: `failure-threshold=5`, `cooldown-ms=30000`.

### TopologyGraph

Thread-safe DAG with `StampedLock`. Reads use `tryOptimisticRead()` with fallback to read lock. Writes take exclusive lock. `StampedLock` is not reentrant — public methods acquire the lock once at the boundary; private helpers contain logic and never re-acquire.

Cycle detection runs Kahn's algorithm (topological sort via in-degree) before committing any new edge.

Stale node eviction: nodes not re-registered within `ripple.ttl.stale-threshold-ms` (120 s) are evicted by a background sweep.

### TokenBucketRateLimiter

Per-caller bucket. Caller identity resolved in priority order: `X-API-Key` header → `X-Forwarded-For` → remote socket IP.

```
tokens = min(tokens + elapsed * rate, maxBurst)
if tokens >= 1: tokens--; accept
else: reject 429 + Retry-After
```

Default: 100 req/s, burst cap 200.

### EventBus

Chronicle Queue ring buffer backed by a file at `ripple.stream.data-dir` (`/tmp/ripple-chronicle`). Single `ExcerptAppender` stamps `eventIndex` on each publish. Each subscriber gets a named `ExcerptTailer` drained by one virtual thread. `BackpressureMonitor` runs every 100 ms and advances slow tailers to queue tail (shedding) when lag exceeds the subscriber's `highWaterMark`.

| Subscriber | High-water mark |
|---|---|
| AlertRouterSubscriber | ∞ (never shed) |
| StateStoreSubscriber | ∞ (never shed) |
| SseSubscriber | 100 (shed early; client recovers via snapshot) |
| DegradationPlannerSubscriber | 1000 (best-effort) |

---

## Configuration

All properties live in `ripple/src/main/resources/application.yml`.

```yaml
ripple:
  probe:
    concurrency-limit: 50          # max in-flight probe threads (Semaphore)
    default-interval-ms: 5000
    http-timeout-ms: 2000
    tcp-timeout-ms: 1000
    default-latency-threshold-ms: 500  # exceed this → DEGRADED (not FAILURE)

  circuit-breaker:
    failure-threshold: 5           # consecutive failures before OPEN
    cooldown-ms: 30000             # OPEN → HALF_OPEN delay

  stream:
    data-dir: /tmp/ripple-chronicle
    sse-high-water-mark: 100
    degradation-planner-high-water-mark: 1000
    backpressure-check-interval-ms: 100

  rate-limit:
    requests-per-second: 100
    max-burst: 200

  blast-radius:
    cascade-threshold: 0.5
    degraded-score-multiplier: 0.5

  steady-state:
    default:
      probe-latency-p99-max-ms: 500
      circuit-breakers-all-closed: true
      subscriber-lag-max: 500

  ttl:
    eviction-interval-ms: 60000
    stale-threshold-ms: 120000
```

---

## Testing

```bash
cd ripple
mvn test                       # unit tests
mvn test -P integration        # integration tests (requires Docker for Testcontainers)
```

| Test class | What it covers |
|---|---|
| `CircuitBreakerTest` | FSM transitions, concurrent CAS correctness |
| `ProbeSchedulerTest` | Virtual thread scheduling, deregistration, exception handling (MockWebServer) |
| `TopologyGraphTest` | DAG invariants, Kahn's cycle detection, concurrent writers |
| `BlastRadiusEngineTest` | Linear chain, fan-out, diamond DAG, score normalization |
| `BackpressureMonitorTest` | Slow subscriber shedding, lag watermark enforcement |
| `TokenBucketRateLimiterTest` | Burst acceptance, rate enforcement, token refill |
| `SteadyStateEvaluatorTest` | Baseline capture, threshold breach assertions |
| `ArchitectureTest` | ArchUnit layer rules, StampedLock reentrancy constraint |

Frameworks: JUnit 5, AssertJ, Awaitility, MockWebServer (OkHttp3), Testcontainers (PostgreSQL), gRPC testing utilities, ArchUnit.

---

## Metrics

Exposed at `/actuator/prometheus`. Key meters:

| Meter | Type | Description |
|---|---|---|
| `probe.latency` | Timer | Per-service probe latency (used by SteadyStateEvaluator for p99) |
| `circuit.breaker.state` | Gauge | 0=CLOSED, 1=OPEN, 2=HALF_OPEN per service |
| `subscriber.lag` | Gauge | Current event lag per subscriber |
| `subscriber.lagging` | Counter | Shedding occurrences per subscriber |

Grafana dashboard available at `localhost:3000` after `docker compose up`.
