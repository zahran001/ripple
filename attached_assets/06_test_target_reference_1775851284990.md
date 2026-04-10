# Ripple — test target reference

## Overview

Ripple is validated against two open-source microservice fleets. Each serves a distinct validation purpose.

| Target | Primary validation | Services | Protocols |
|---|---|---|---|
| Google Online Boutique | Topology accuracy, blast radius correctness, backpressure under load | 11 | gRPC + HTTP |
| OpenTelemetry Astronomy Shop | Blast radius cross-validation against real OTel traces | 22 | HTTP + gRPC |

---

## Target 1 — Google Online Boutique

**Repository:** https://github.com/GoogleCloudPlatform/microservices-demo

**What it is:** A cloud-native e-commerce demo with 11 polyglot services (Go, Python, Java, Node.js, C#). Purpose-built to demonstrate microservice communication patterns over gRPC. Ships with a built-in load generator.

### Service dependency graph (expected topology)

This is the ground truth against which Ripple's auto-discovered topology is compared.

```
frontend
├── productcatalogservice
├── currencyservice
├── cartservice
│   └── redis
├── recommendationservice
│   └── productcatalogservice
├── checkoutservice
│   ├── productcatalogservice
│   ├── cartservice
│   ├── currencyservice
│   ├── emailservice
│   ├── paymentservice
│   └── shippingservice
└── adservice
```

### Expected blast radius per service failure

Use this table to verify `BlastRadiusEngine` output during Phase 3.

| Failed service | Expected affected services | Max depth |
|---|---|---|
| `currencyservice` | `frontend`, `checkoutservice` | 1 |
| `productcatalogservice` | `frontend`, `recommendationservice`, `checkoutservice` | 1 |
| `cartservice` | `frontend`, `checkoutservice` | 1 |
| `paymentservice` | `checkoutservice`, `frontend` | 2 |
| `emailservice` | `checkoutservice`, `frontend` | 2 |
| `shippingservice` | `checkoutservice`, `frontend` | 2 |
| `adservice` | `frontend` | 1 |
| `recommendationservice` | `frontend` | 1 |
| `checkoutservice` | `frontend` | 1 |
| `redis` | `cartservice`, `checkoutservice`, `frontend` | 3 |

### Validation scenarios

**Phase 2 — Topology accuracy**

Run Online Boutique via `docker-compose`. Point Ripple's discovery endpoint at the fleet. After all services register, compare Ripple's `GET /topology` response against the dependency graph above. Every edge must match.

**Phase 3 — Blast radius correctness**

Use Ripple's simulation endpoint (`POST /simulate`) to mark each service as hypothetically failed. Compare the returned `AffectedSet` against the expected affected services table above. All must match before Phase 3 is considered complete.

**Phase 4 — Backpressure under load**

Start the Online Boutique load generator (`loadgenerator` service). Run a 10-minute soak test. During this period:
- Introduce an artificial slow SSE subscriber (adds 500ms delay per event).
- Verify the slow subscriber is shed at the high-water mark.
- Verify fast subscribers (alert router, state store) receive all events.
- Verify no memory leak (heap usage stable over the 10-minute window).

### docker-compose integration

```yaml
# Add to Online Boutique's docker-compose.yml or use an override file
ripple:
  image: ripple:latest
  ports:
    - "8080:8080"
  environment:
    RIPPLE_PROBE_DEFAULT_INTERVAL_MS: 5000
    SPRING_DATASOURCE_URL: jdbc:postgresql://postgres:5432/ripple
    SPRING_REDIS_HOST: redis
  depends_on:
    - postgres
    - redis
```

Services register with Ripple via a sidecar call on startup. For Online Boutique services we cannot modify, use Ripple's manual registration API to pre-populate the topology.

---

## Target 2 — OpenTelemetry Astronomy Shop

**Repository:** https://github.com/open-telemetry/opentelemetry-demo

**What it is:** A 22-service astronomy-themed e-commerce demo with OpenTelemetry instrumentation baked into every service. Designed to showcase distributed tracing, metrics, and logging across a polyglot fleet.

**Validation role:** Cross-reference Ripple's blast radius predictions against real distributed traces. If Ripple predicts Service X degrades when Service Y fails, OTel traces should show Service X's spans erroring or timing out when Y is killed.

### Service map (abbreviated)

The Astronomy Shop has 22 services. Key dependency chains for validation:

```
frontend
└── productcatalogservice
    └── featureflagservice
checkout
├── productcatalogservice
├── cartservice
│   └── redis
├── currencyservice
├── emailservice
├── paymentservice
├── shippingservice
└── quoteservice
recommendationservice
└── productcatalogservice
```

Full service map: https://opentelemetry.io/docs/demo/architecture/

### Cross-validation methodology

1. Start Astronomy Shop + Jaeger (distributed tracing UI) + Ripple.
2. Run the load generator for 5 minutes to establish baseline traces.
3. Kill a target service (e.g., `productcatalogservice`).
4. Record Ripple's blast radius prediction within 30 seconds.
5. Pull traces from Jaeger for the 60 seconds following the kill.
6. Compare: every service Ripple predicted as affected should appear in Jaeger traces with `error=true` or elevated latency.
7. Verify no false positives: services Ripple did not predict as affected should show healthy traces.

### Validation checklist

For each killed service, record:

- [ ] Ripple predicted affected set (list services)
- [ ] OTel trace confirmed affected set (list services with errors in Jaeger)
- [ ] False positives (Ripple predicted affected, traces show healthy)
- [ ] False negatives (Ripple did not predict, traces show errors)
- [ ] Time from service kill to Ripple blast radius update (target: < 30s)

A false negative is a bug in the topology graph or BFS. A false positive is a bug in the dependency registration. Both must be tracked and resolved.

---

## Failure injection

### Manual kill (docker-compose)

```bash
docker-compose stop productcatalogservice
# observe Ripple dashboard
docker-compose start productcatalogservice
```

### Scripted chaos sequence (Phase 6)

```bash
#!/bin/bash
# chaos.sh — sequential failure injection for demo
SERVICES=("currencyservice" "productcatalogservice" "cartservice" "paymentservice")

for svc in "${SERVICES[@]}"; do
  echo "Killing $svc..."
  docker-compose stop "$svc"
  sleep 30   # observe blast radius
  echo "Restoring $svc..."
  docker-compose start "$svc"
  sleep 15   # allow recovery
done
```

---

## Known topology gaps

Services that communicate in ways not captured by standard registration:

- `redis` — not a Ripple-registered service; injected as a manual node with edges from `cartservice`
- `loadgenerator` — excluded from topology (it's a producer, not a dependency)
- Async communication (message queues, if any) — not captured by synchronous probe registration; document as a known limitation
