package com.ripple.engine.probe;

/**
 * States of the hand-rolled circuit breaker state machine.
 *
 * <p>Transitions:
 * <pre>
 * CLOSED    ──(N consecutive failures)──► OPEN
 * OPEN      ──(cooldown elapsed)────────► HALF_OPEN
 * HALF_OPEN ──(probe succeeds)───────────► CLOSED
 * HALF_OPEN ──(probe fails)──────────────► OPEN
 * </pre>
 */
public enum CircuitBreakerState {
    CLOSED,
    OPEN,
    HALF_OPEN
}
