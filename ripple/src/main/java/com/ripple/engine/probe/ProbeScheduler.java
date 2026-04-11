package com.ripple.engine.probe;

import com.ripple.config.CircuitBreakerProperties;
import com.ripple.config.ProbeProperties;
import com.ripple.model.ProbeResult;
import com.ripple.model.ProbeStatus;
import com.ripple.model.ServiceNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Continuously schedules health-check probes for all registered services.
 *
 * <p><strong>Concurrency model:</strong>
 * <ul>
 *   <li>One virtual thread per probe <em>attempt</em> — not per service. Threads are
 *       cheap; no pool sizing required.</li>
 *   <li>A {@link Semaphore} caps the total number of concurrent in-flight probes to
 *       prevent thundering herds. The cap is logged on startup and derived from config.</li>
 *   <li>{@link CircuitBreaker} state is {@code AtomicReference} — no locking.</li>
 * </ul>
 *
 * <p>The only shared mutable state in this layer is the {@code Semaphore} and the two
 * {@link ConcurrentHashMap}s ({@code services}, {@code circuitBreakers}).
 */
public final class ProbeScheduler implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ProbeScheduler.class);

    private final ProbeProperties probeProps;
    private final CircuitBreakerProperties cbProps;
    private final Consumer<ProbeResult> resultConsumer;

    // guarded by ConcurrentHashMap — thread-safe insertions and lookups
    private final ConcurrentHashMap<String, ServiceNode> services = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Prober> probers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();

    // single-thread scheduler drives the per-interval probe fan-out
    private final ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("probe-scheduler").factory()
        );

    // caps total concurrent in-flight probes — the only shared lock in this layer
    private final Semaphore concurrencySemaphore;

    public ProbeScheduler(ProbeProperties probeProps,
                          CircuitBreakerProperties cbProps,
                          Consumer<ProbeResult> resultConsumer) {
        this.probeProps = probeProps;
        this.cbProps = cbProps;
        this.resultConsumer = resultConsumer;
        this.concurrencySemaphore = new Semaphore(probeProps.concurrencyLimit());
        log.info("ProbeScheduler started — concurrency cap: {}", probeProps.concurrencyLimit());
    }

    /**
     * Registers a service for continuous health-checking.
     * Schedules the first probe after one interval, then repeats at the configured rate.
     */
    public void register(ServiceNode node, Prober prober) {
        String key = node.id().value();
        services.put(key, node);
        probers.put(key, prober);
        circuitBreakers.put(key, new CircuitBreaker(
            CircuitBreakerConfig.from(cbProps),
            newState -> log.warn("Circuit breaker [{}] → {}", node.id(), newState)
        ));

        long intervalMs = node.probeInterval().toMillis();
        scheduler.scheduleAtFixedRate(
            () -> spawnProbeVirtualThread(node),
            intervalMs,
            intervalMs,
            TimeUnit.MILLISECONDS
        );
        log.info("Registered service {} for probing every {}ms", node.id(), intervalMs);
    }

    /**
     * Deregisters a service. Stops future probes; does not cancel any in-flight probe.
     */
    public void deregister(String serviceId) {
        services.remove(serviceId);
        probers.remove(serviceId);
        var breaker = circuitBreakers.remove(serviceId);
        if (breaker != null) breaker.close();
    }

    /**
     * Returns the circuit breaker for the given service, or {@code null} if not registered.
     * Intended for the health endpoint and metrics export.
     */
    public CircuitBreaker circuitBreakerFor(String serviceId) {
        return circuitBreakers.get(serviceId);
    }

    /** Spawns one virtual thread to execute the probe. Returns immediately. */
    private void spawnProbeVirtualThread(ServiceNode node) {
        Thread.ofVirtual()
            .name("probe-" + node.id().value())
            .start(() -> {
                try {
                    concurrencySemaphore.acquire();
                    try {
                        executeProbe(node);
                    } finally {
                        concurrencySemaphore.release();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Probe thread interrupted for {}", node.id());
                }
            });
    }

    /**
     * Executes a single probe attempt. Checks the circuit breaker state first:
     * if OPEN, emits a CIRCUIT_OPEN result without touching the network.
     * Measures wall-clock latency and compares against the node's threshold.
     */
    private void executeProbe(ServiceNode node) {
        String key = node.id().value();
        CircuitBreaker breaker = circuitBreakers.get(key);
        if (breaker == null) return; // deregistered between schedule and execution

        if (breaker.state() == CircuitBreakerState.OPEN) {
            resultConsumer.accept(ProbeResult.circuitOpen(node.id(), Instant.now()));
            return;
        }

        Prober prober = probers.get(key);
        if (prober == null) return;

        long startNanos = System.nanoTime();
        ProbeResult rawResult;
        try {
            rawResult = prober.probe(node);
        } catch (Exception probeException) {
            // Prober threw — treat as FAILURE. Catching here prevents the exception from
            // propagating as an uncaught virtual-thread exception (which Awaitility 4
            // captures by default and surfaces as a test ERROR).
            Duration latencyOnException = Duration.ofNanos(System.nanoTime() - startNanos);
            log.warn("Prober for [{}] threw {}: {}", node.id(),
                probeException.getClass().getSimpleName(), probeException.getMessage());
            breaker.recordFailure();
            resultConsumer.accept(ProbeResult.failure(
                node.id(), latencyOnException, probeException.getMessage()));
            return;
        }
        Duration latency = Duration.ofNanos(System.nanoTime() - startNanos);

        // Apply latency threshold — SUCCESS that exceeds threshold becomes DEGRADED
        ProbeStatus status = rawResult.status();
        if (status == ProbeStatus.SUCCESS && latency.toMillis() > node.latencyThresholdMs()) {
            status = ProbeStatus.DEGRADED;
            log.warn("Service {} responded in {}ms — exceeds threshold {}ms → DEGRADED",
                node.id(), latency.toMillis(), node.latencyThresholdMs());
        }

        // Update circuit breaker
        if (status == ProbeStatus.SUCCESS) {
            breaker.recordSuccess();
        } else if (status == ProbeStatus.FAILURE || status == ProbeStatus.DEGRADED) {
            breaker.recordFailure();
        }

        ProbeResult finalResult = new ProbeResult(
            node.id(), status, latency, Instant.now(),
            rawResult.detail()
        );
        resultConsumer.accept(finalResult);
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        circuitBreakers.values().forEach(CircuitBreaker::close);
        log.info("ProbeScheduler shut down");
    }
}
