package com.ripple.engine.steadystate;

import com.ripple.engine.probe.CircuitBreakerState;
import com.ripple.engine.probe.ProbeScheduler;
import com.ripple.engine.stream.BackpressureMonitor;
import com.ripple.engine.steadystate.SteadyStateResult.AssertionOutcome;
import com.ripple.engine.steadystate.SteadyStateResult.SteadyStateStatus;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Evaluates a named steady-state hypothesis against live Micrometer metrics.
 *
 * <p><strong>Why Micrometer directly:</strong> Ripple already exports all relevant
 * metrics (probe latencies, circuit breaker states, subscriber lag) via Micrometer.
 * Reading them from the {@code MeterRegistry} in-process is zero-latency and requires
 * no external dependency. The alternative — querying Prometheus — would add an HTTP
 * round-trip and a dependency on Prometheus being up, which is ironic for a health tool.
 *
 * <p><strong>Assertions evaluated:</strong>
 * <ol>
 *   <li>Probe latency p99 — checks the {@code probe.latency} timer's 99th percentile.</li>
 *   <li>Circuit breakers all closed — checks that no registered circuit is in OPEN or HALF_OPEN.</li>
 *   <li>Subscriber lag — checks that no subscriber lag exceeds the configured maximum.</li>
 * </ol>
 *
 * <p>Thread-safe — reads only; MeterRegistry reads are safe from any thread.
 */
@Service
public final class SteadyStateEvaluator {

    private static final Logger log = LoggerFactory.getLogger(SteadyStateEvaluator.class);

    private final MeterRegistry meterRegistry;
    private final ProbeScheduler probeScheduler;
    private final BackpressureMonitor backpressureMonitor;

    // Known subscriber names for lag checks
    private static final List<String> SUBSCRIBER_NAMES =
        List.of("alert-router", "state-store", "sse-feed", "degradation-planner");

    public SteadyStateEvaluator(MeterRegistry meterRegistry,
                                ProbeScheduler probeScheduler,
                                BackpressureMonitor backpressureMonitor) {
        this.meterRegistry = meterRegistry;
        this.probeScheduler = probeScheduler;
        this.backpressureMonitor = backpressureMonitor;
    }

    /**
     * Evaluates the given hypothesis definition and returns a structured result with
     * per-assertion detail.
     *
     * @param hypothesisName  the name of the hypothesis being evaluated (for labelling)
     * @param definition      the threshold configuration to evaluate against
     * @return STEADY if all assertions pass; VIOLATED if any assertion fails
     */
    public SteadyStateResult evaluate(String hypothesisName, SteadyStateDefinition definition) {
        List<AssertionOutcome> outcomes = new ArrayList<>();

        // Assertion 1: probe latency p99
        outcomes.add(assertProbeLatency(definition.probeLatencyP99MaxMs()));

        // Assertion 2: circuit breakers all closed
        if (definition.circuitBreakersAllClosed()) {
            outcomes.add(assertCircuitBreakers());
        }

        // Assertion 3: subscriber lag
        outcomes.add(assertSubscriberLag(definition.subscriberLagMax()));

        boolean allPassed = outcomes.stream().allMatch(AssertionOutcome::passed);
        SteadyStateResult result = allPassed
            ? SteadyStateResult.steady(hypothesisName, outcomes)
            : SteadyStateResult.violated(hypothesisName, outcomes);

        log.info("Steady-state hypothesis [{}]: {}", hypothesisName, result.status());
        return result;
    }

    // =====================================================================
    // Individual assertions
    // =====================================================================

    private AssertionOutcome assertProbeLatency(long maxMs) {
        Timer timer = meterRegistry.find("probe.latency").timer();
        if (timer == null) {
            return new AssertionOutcome("probe.latency.p99", true,
                "no probe latency data yet — treating as passed");
        }

        double p99Ms = timer.percentile(0.99, TimeUnit.MILLISECONDS);
        boolean passed = p99Ms <= maxMs;
        String detail = "p99=%.1fms, max=%dms".formatted(p99Ms, maxMs);
        return new AssertionOutcome("probe.latency.p99", passed, detail);
    }

    private AssertionOutcome assertCircuitBreakers() {
        // Check circuit breaker state metrics registered by ProbeScheduler
        var openCircuits = new ArrayList<String>();
        meterRegistry.find("circuit.breaker.state").gauges().forEach(gauge -> {
            double stateValue = gauge.value();
            // Convention: 0=CLOSED, 1=OPEN, 2=HALF_OPEN
            if (stateValue > 0) {
                openCircuits.add(gauge.getId().getTag("service") + "=" +
                    (stateValue == 1.0 ? "OPEN" : "HALF_OPEN"));
            }
        });

        boolean passed = openCircuits.isEmpty();
        String detail = passed ? "all circuit breakers CLOSED"
            : "open circuits: " + String.join(", ", openCircuits);
        return new AssertionOutcome("circuit.breakers.all-closed", passed, detail);
    }

    private AssertionOutcome assertSubscriberLag(long maxLag) {
        long worstLag = 0;
        String worstSubscriber = "none";

        for (String subscriberName : SUBSCRIBER_NAMES) {
            long lag = backpressureMonitor.lagFor(subscriberName);
            if (lag > worstLag) {
                worstLag = lag;
                worstSubscriber = subscriberName;
            }
        }

        boolean passed = worstLag <= maxLag;
        String detail = "worst lag: %d events on [%s], max=%d".formatted(worstLag, worstSubscriber, maxLag);
        return new AssertionOutcome("subscriber.lag", passed, detail);
    }
}
