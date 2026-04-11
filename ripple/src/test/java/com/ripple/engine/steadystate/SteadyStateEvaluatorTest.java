package com.ripple.engine.steadystate;

import com.ripple.config.CircuitBreakerProperties;
import com.ripple.config.ProbeProperties;
import com.ripple.engine.probe.ProbeScheduler;
import com.ripple.engine.stream.BackpressureMonitor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
}
