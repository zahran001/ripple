package com.ripple.engine.steadystate;

import java.time.Instant;
import java.util.List;

/**
 * Result of evaluating a named steady-state hypothesis.
 */
public record SteadyStateResult(
    String hypothesisName,
    SteadyStateStatus status,
    List<AssertionOutcome> assertions,
    Instant evaluatedAt
) {

    public enum SteadyStateStatus {
        STEADY,
        VIOLATED
    }

    public record AssertionOutcome(
        String name,
        boolean passed,
        String detail
    ) {}

    public static SteadyStateResult steady(String name, List<AssertionOutcome> assertions) {
        return new SteadyStateResult(name, SteadyStateStatus.STEADY, assertions, Instant.now());
    }

    public static SteadyStateResult violated(String name, List<AssertionOutcome> assertions) {
        return new SteadyStateResult(name, SteadyStateStatus.VIOLATED, assertions, Instant.now());
    }
}
