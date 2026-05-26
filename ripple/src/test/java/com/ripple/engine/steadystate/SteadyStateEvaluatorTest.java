package com.ripple.engine.steadystate;

import com.ripple.config.CircuitBreakerProperties;
import com.ripple.config.ProbeProperties;
import com.ripple.engine.probe.ProbeScheduler;
import com.ripple.engine.stream.BackpressureMonitor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link SteadyStateEvaluator}.
 *
 * <p>Tests the three assertions in isolation and in combination:
 * probe latency p99, circuit breakers all closed, subscriber lag.
 */
class SteadyStateEvaluatorTest {

    private MeterRegistry meterRegistry;
    private ProbeScheduler probeScheduler;
    private BackpressureMonitor backpressureMonitor;
    private SteadyStateEvaluator evaluator;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        probeScheduler = Mockito.mock(ProbeScheduler.class);
        backpressureMonitor = Mockito.mock(BackpressureMonitor.class);

        // Default: no lag
        Mockito.when(backpressureMonitor.lagFor(Mockito.anyString())).thenReturn(0L);

        evaluator = new SteadyStateEvaluator(meterRegistry, probeScheduler, backpressureMonitor);
    }

    @Test
    void steady_when_no_metrics_present() {
        // No probe data yet → treating as passed
        var definition = new SteadyStateDefinition(500, true, 500);
        var result = evaluator.evaluate("baseline", definition);

        assertThat(result.status()).isEqualTo(SteadyStateResult.SteadyStateStatus.STEADY);
    }

    @Test
    void violated_when_subscriber_lag_exceeds_max() {
        Mockito.when(backpressureMonitor.lagFor("sse-feed")).thenReturn(1000L); // above 500 max

        var definition = new SteadyStateDefinition(500, true, 500);
        var result = evaluator.evaluate("lagging", definition);

        assertThat(result.status()).isEqualTo(SteadyStateResult.SteadyStateStatus.VIOLATED);

        boolean lagAssertionFailed = result.assertions().stream()
            .anyMatch(a -> a.name().equals("subscriber.lag") && !a.passed());
        assertThat(lagAssertionFailed).isTrue();
    }

    @Test
    void steady_when_lag_within_limit() {
        Mockito.when(backpressureMonitor.lagFor(Mockito.anyString())).thenReturn(50L);

        var definition = new SteadyStateDefinition(500, true, 500);
        var result = evaluator.evaluate("ok", definition);

        assertThat(result.status()).isEqualTo(SteadyStateResult.SteadyStateStatus.STEADY);
    }

    @Test
    void result_includes_all_assertion_outcomes() {
        var definition = new SteadyStateDefinition(500, true, 500);
        var result = evaluator.evaluate("full", definition);

        // At least 3 assertions evaluated
        assertThat(result.assertions()).hasSizeGreaterThanOrEqualTo(3);
        assertThat(result.hypothesisName()).isEqualTo("full");
        assertThat(result.evaluatedAt()).isNotNull();
    }

    @Test
    void circuit_breaker_assertion_skipped_when_disabled_in_definition() {
        var definition = new SteadyStateDefinition(500, false, 500); // circuit check disabled
        var result = evaluator.evaluate("no-circuit-check", definition);

        boolean circuitAssertionPresent = result.assertions().stream()
            .anyMatch(a -> a.name().equals("circuit.breakers.all-closed"));
        assertThat(circuitAssertionPresent).isFalse();
    }

    @Test
    void violated_when_probe_latency_p99_exceeds_max() {
        // Register timer exactly as production does (with publishPercentiles).
        // Record 50 fast (10ms) + 50 slow (2000ms) samples — p99 resolves to ~2000ms,
        // well above the 500ms threshold. (SimpleMeterRegistry uses a coarse fixed-bucket
        // histogram; a single slow sample in 100 gets compressed below threshold.)
        Timer timer = Timer.builder("probe.latency")
            .tag("service", "test-svc")
            .publishPercentiles(0.99, 0.95, 0.50)
            .register(meterRegistry);
        for (int i = 0; i < 50; i++) timer.record(10, TimeUnit.MILLISECONDS);
        for (int i = 0; i < 50; i++) timer.record(2000, TimeUnit.MILLISECONDS);

        var definition = new SteadyStateDefinition(500, false, 500);
        var result = evaluator.evaluate("high-latency", definition);

        assertThat(result.status()).isEqualTo(SteadyStateResult.SteadyStateStatus.VIOLATED);
        boolean latencyFailed = result.assertions().stream()
            .anyMatch(a -> a.name().equals("probe.latency.p99") && !a.passed());
        assertThat(latencyFailed).isTrue();
    }

    @Test
    void violated_when_circuit_breaker_is_open() {
        // Register a gauge with value 1.0 (OPEN) as the production code does
        meterRegistry.gauge("circuit.breaker.state",
            java.util.List.of(io.micrometer.core.instrument.Tag.of("service", "test-svc")),
            1.0);

        var definition = new SteadyStateDefinition(500, true, 500);
        var result = evaluator.evaluate("open-circuit", definition);

        assertThat(result.status()).isEqualTo(SteadyStateResult.SteadyStateStatus.VIOLATED);
        boolean circuitFailed = result.assertions().stream()
            .anyMatch(a -> a.name().equals("circuit.breakers.all-closed") && !a.passed());
        assertThat(circuitFailed).isTrue();
    }
}
