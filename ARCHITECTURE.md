# Ripple — Architecture Diagrams

Two complementary views of how the system works. Read Diagram 1 for the runtime flow, Diagram 2 for the structural map.

---

## Diagram 1 — End-to-End Event Lifecycle

How a single probe tick becomes a live SSE event on the dashboard. Follow the numbered steps.

```mermaid
sequenceDiagram
    autonumber
    participant PS as ProbeScheduler
    participant CB as CircuitBreaker
    participant P as Prober
    participant RO as RippleOrchestrator
    participant TG as TopologyGraph
    participant BR as BlastRadiusEngine
    participant EB as EventBus
    participant SS as StateStoreSubscriber
    participant AR as AlertRouterSubscriber
    participant SSE as SseSubscriber
    participant DP as DegradationPlanner
    participant BM as BackpressureMonitor
    participant DC as DashboardClient

    Note over PS: Every N ms per service — virtual thread, semaphore-capped

    PS->>CB: state()?
    alt Circuit OPEN
        CB-->>PS: OPEN
        PS->>RO: handleProbeResult — status=CIRCUIT_OPEN
        Note over RO: No event emitted
    else Circuit CLOSED or HALF_OPEN
        CB-->>PS: proceed
        PS->>P: probe(ServiceNode) — HTTP / gRPC / TCP
        P-->>PS: ProbeResult — status + latency
        Note over PS: SUCCESS: latency under threshold<br>DEGRADED: latency over threshold<br>FAILURE: unreachable / timeout
        PS->>CB: recordSuccess() or recordFailure()
        Note over CB: CLOSED → OPEN after N consecutive failures<br>OPEN → HALF_OPEN after cooldown<br>HALF_OPEN → CLOSED on next success
        PS->>RO: handleProbeResult(ProbeResult)
    end

    RO->>RO: liveProbeStatuses.put(serviceId, newStatus)
    RO->>RO: determineEventType(previousStatus, newStatus)
    Note over RO: Returns null if status unchanged — exits here

    alt Status is FAILURE or DEGRADED
        RO->>TG: snapshot()
        TG-->>RO: GraphSnapshot — immutable deep-copy, no locks needed
        RO->>BR: compute(serviceId, snapshot, liveStatuses)
        Note over BR: Parallel BFS via ForkJoinPool<br>score = criticality / (depth + 1)<br>DEGRADED nodes multiplied by 0.5<br>All scores normalized to 0–1
        BR-->>RO: BlastRadiusResult — affected services ordered by severity
        RO->>EB: publish — FailureEvent with blastRadius attached
    else Status is RECOVERED or topology change
        RO->>EB: publish — FailureEvent, blastRadius null
    end

    EB->>EB: serialize to JSON, append to Chronicle Queue
    Note over EB: Stamps event with Chronicle index as eventIndex<br>Increments produced counter

    par Fan-out — each subscriber has its own drain thread
        EB->>SS: onEvent
        SS->>SS: INSERT into failure_events — PostgreSQL
        SS->>SS: SET service health key in Redis — 300s TTL
    and
        EB->>AR: onEvent
        Note over AR: Fires only for SERVICE_FAILURE and CIRCUIT_OPENED
        AR->>AR: POST to webhook — up to 5 retries, exponential backoff 1s to 60s
    and
        EB->>SSE: onEvent
        SSE->>SSE: tryEmitNext into hot Flux sink
    and
        EB->>DP: onEvent
        DP->>DP: match runbooks by serviceId<br>sort suggestions by severity DESC<br>push to API-layer cache
    end

    loop Every 100 ms
        BM->>EB: read produced and consumed counts per subscriber
        alt lag exceeds high-water mark
            Note over BM: SseSubscriber HWM = 100 — shed early, client recovers<br>AlertRouter and StateStore HWM = MAX — never shed
            BM->>EB: advance tailer to tail — reset consumed to produced
        end
    end

    DC->>SSE: GET /events/stream
    SSE-->>DC: ServerSentEvent — id=eventIndex, event=type, data=JSON
    Note over DC: If id jumps by more than 1 — gap detected<br>Client calls GET /topology to resync<br>Then resumes live stream
```

---

## Diagram 2 — Structural Layers

What each layer owns and how the two main flows (probe loop and SSE delivery) connect them.

```mermaid
flowchart TD
    subgraph CLIENT["Dashboard Client"]
        DC["Browser\nGET /events/stream\nGET /topology (gap recovery)"]
    end

    subgraph API["HTTP API Layer  (RateLimitInterceptor on all routes)"]
        DISC["DiscoveryController\nPOST /topology/services\nPOST /topology/edges\nGET  /topology"]
        BLAST["BlastRadiusController\nGET /blast-radius/{id}\nPOST /blast-radius/{id}/simulate\nPOST /blast-radius/{id}/cascade"]
        SS["SteadyStateController\nPOST /steady-state/{name}/baseline\nPOST /steady-state/{name}/evaluate"]
        HEALTH["HealthController\nGET /health\nGET /health/circuit-breakers\nGET /health/subscribers"]
        SSEC["SseController\nGET /events/stream"]
    end

    subgraph ENGINE["Engine Layer"]
        TG["TopologyGraph\nStampedLock DAG\nCycle detection (Kahn's)"]
        PS["ProbeScheduler\nVirtual thread per probe\nSemaphore concurrency cap"]
        CB["CircuitBreaker\nAtomicRef FSM\nCLOSED → OPEN → HALF_OPEN"]
        BRE["BlastRadiusEngine\nParallel BFS (ForkJoin)\nscore = criticality ÷ (depth+1)"]
        EVB["EventBus\nChronicle Queue\nSingle appender · N tailers\neventIndex stamps"]
        BPM["BackpressureMonitor\n@Scheduled every 100ms\nlag = produced − consumed"]
        SSE_E["SteadyStateEvaluator\np99 latency · CB states · sub lag"]
    end

    subgraph SUBS["Subscriber Layer  (virtual-thread drain loops)"]
        STS["StateStoreSubscriber\nHWM = MAX (never shed)"]
        ARS["AlertRouterSubscriber\nHWM = MAX (never shed)"]
        SSES["SseSubscriber\nHWM = 100 (shed early)"]
        DPS["DegradationPlannerSubscriber\nHWM = 1000"]
    end

    subgraph STORE["Storage"]
        PG[("PostgreSQL\nevent history")]
        REDIS[("Redis\nhealth cache · 5 min TTL")]
        CQ[("Chronicle Queue\noff-heap ring buffer")]
    end

    DC -->|"register service/edge"| DISC
    DISC --> TG

    PS -->|"check state"| CB
    CB -->|"proceed / skip"| PS
    PS -->|"ProbeResult callback"| RO["RippleOrchestrator\n(wires PS → BR → EventBus)"]
    RO -->|"snapshot()"| TG
    RO -->|"compute()"| BRE
    RO -->|"publish(FailureEvent)"| EVB

    EVB --> STS
    EVB --> ARS
    EVB --> SSES
    EVB --> DPS
    BPM -->|"lag check / shed"| EVB

    STS --> PG
    STS --> REDIS
    ARS -->|"POST webhook"| EXT["External Alerts\n(PagerDuty / Slack)"]

    SSES -->|"hot Flux"| SSEC
    SSEC --> DC

    HEALTH -->|"lagFor()"| BPM
    HEALTH -->|"CB states"| CB
    BLAST --> BRE
    SS --> SSE_E
    EVB --> CQ
```

---

## Key Insight

Every component from `ProbeScheduler` through `BlastRadiusEngine` exists to produce one well-formed `FailureEvent` with a correct `eventIndex`. Everything after `EventBus.publish()` is fan-out — each subscriber reads the same event independently at its own pace, without blocking any other.

The two short-circuit exits that prevent unnecessary work:
1. **Circuit breaker OPEN** → probe skipped entirely, no network call made
2. **Status unchanged** → `determineEventType()` returns null, no event published
