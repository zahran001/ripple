package com.ripple.engine.probe;

import com.ripple.config.CircuitBreakerProperties;
import com.ripple.config.ProbeProperties;
import com.ripple.model.ProbeResult;
import com.ripple.model.ProbeStatus;
import com.ripple.model.Protocol;
import com.ripple.model.ServiceId;
import com.ripple.model.ServiceNode;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link ProbeScheduler}.
 *
 * <p>Uses a no-op {@link Prober} implementation to test scheduling behaviour
 * without any real network calls. The {@link CircuitBreaker} is exercised through
 * the scheduler's failure recording path.
 */
class ProbeSchedulerTest {

    private ProbeScheduler scheduler;
    private final CopyOnWriteArrayList<ProbeResult> results = new CopyOnWriteArrayList<>();

    private final ProbeProperties probeProps =
        new ProbeProperties(10, 100, 2000, 1000, 500);

    private final CircuitBreakerProperties cbProps =
        new CircuitBreakerProperties(3, 100); // fast cooldown for testing

    @BeforeEach
    void setUp() {
        results.clear();
        scheduler = new ProbeScheduler(probeProps, cbProps, results::add);
    }

    @AfterEach
    void tearDown() {
        scheduler.close();
    }

    private ServiceNode serviceNode(String id, long intervalMs) {
        return ServiceNode.of(
            ServiceId.of(id), id,
            Protocol.HTTP,
            URI.create("http://localhost:9999/" + id),
            Duration.ofMillis(intervalMs),
            500L
        );
    }

    @Test
    void schedules_probes_at_configured_interval() {
        AtomicInteger probeCount = new AtomicInteger(0);
        Prober countingProber = node -> {
            probeCount.incrementAndGet();
            return ProbeResult.success(node.id(), Duration.ofMillis(10));
        };

        scheduler.register(serviceNode("svc-1", 100), countingProber);

        Awaitility.await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> assertThat(probeCount.get()).isGreaterThanOrEqualTo(3));
    }

    @Test
    void delivers_results_to_consumer() {
        Prober successProber = node -> ProbeResult.success(node.id(), Duration.ofMillis(10));
        scheduler.register(serviceNode("svc-deliver", 100), successProber);

        Awaitility.await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> assertThat(results).isNotEmpty());

        assertThat(results).allMatch(r -> r.serviceId().value().equals("svc-deliver"));
    }

    @Test
    void marks_as_degraded_when_latency_exceeds_threshold() {
        // Prober returns success but the latency measurement exceeds the threshold
        // We control the ServiceNode's latencyThresholdMs = 1ms, and the prober is slow
        ServiceNode slowNode = new ServiceNode(
            ServiceId.of("slow-svc"), "slow-svc",
            Protocol.HTTP,
            URI.create("http://localhost:9999/slow"),
            5, Duration.ofMillis(100), 1L // 1ms threshold — anything slower is DEGRADED
        );

        Prober slowProber = node -> {
            try { Thread.sleep(5); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return ProbeResult.success(node.id(), Duration.ofMillis(5));
        };

        scheduler.register(slowNode, slowProber);

        Awaitility.await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() ->
                assertThat(results).anyMatch(r -> r.status() == ProbeStatus.DEGRADED)
            );
    }

    @Test
    void skips_probe_when_circuit_open() {
        AtomicInteger probeCount = new AtomicInteger(0);
        Prober failingProber = node -> {
            probeCount.incrementAndGet();
            return ProbeResult.failure(node.id(), Duration.ofMillis(100), "connection refused");
        };

        ServiceNode node = serviceNode("failing-svc", 100);
        scheduler.register(node, failingProber);

        // Wait for circuit to open (3 failures configured in cbProps)
        Awaitility.await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() -> {
                CircuitBreaker cb = scheduler.circuitBreakerFor("failing-svc");
                assertThat(cb).isNotNull();
                assertThat(cb.state()).isEqualTo(CircuitBreakerState.OPEN);
            });

        // After circuit opens, CIRCUIT_OPEN results should appear (no real probe calls)
        int countAfterOpen = probeCount.get();
        Awaitility.await()
            .atMost(Duration.ofSeconds(1))
            .untilAsserted(() ->
                assertThat(results).anyMatch(r -> r.status() == ProbeStatus.CIRCUIT_OPEN)
            );
    }

    @Test
    void handles_prober_exception_as_failure() {
        Prober throwingProber = node -> {
            throw new RuntimeException("simulated network error");
        };

        scheduler.register(serviceNode("throwing-svc", 100), throwingProber);

        Awaitility.await()
            .atMost(Duration.ofSeconds(2))
            .untilAsserted(() ->
                assertThat(results).anyMatch(r ->
                    r.status() == ProbeStatus.FAILURE &&
                    r.serviceId().value().equals("throwing-svc"))
            );
    }

    @Test
    void deregister_stops_future_probes() throws InterruptedException {
        AtomicInteger count = new AtomicInteger(0);
        Prober countingProber = node -> {
            count.incrementAndGet();
            return ProbeResult.success(node.id(), Duration.ofMillis(10));
        };

        scheduler.register(serviceNode("deregister-svc", 100), countingProber);
        Thread.sleep(250);

        scheduler.deregister("deregister-svc");
        int countAfterDeregister = count.get();

        Thread.sleep(300);
        // Count should not increase significantly after deregistration
        // (allow for one in-flight probe)
        assertThat(count.get()).isLessThanOrEqualTo(countAfterDeregister + 1);
    }
}
