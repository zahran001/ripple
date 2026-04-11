package com.ripple.engine.steadystate;

/**
 * Immutable definition of a named steady-state hypothesis.
 *
 * <p>Bound from {@code ripple.steady-state.{name}.*} in {@code application.yml}.
 * Different test scenarios can have different steady-state definitions — a latency
 * soak test may use a stricter p99 threshold than a topology registration test.
 *
 * @param probLatencyP99MaxMs  maximum acceptable p99 probe latency in ms
 * @param circuitBreakersAllClosed  true if the hypothesis requires all circuits to be CLOSED
 * @param subscriberLagMax     maximum acceptable lag for any single subscriber
 */
public record SteadyStateDefinition(
    long probeLatencyP99MaxMs,
    boolean circuitBreakersAllClosed,
    long subscriberLagMax
) {

    public static SteadyStateDefinition defaults() {
        return new SteadyStateDefinition(500L, true, 500L);
    }
}
