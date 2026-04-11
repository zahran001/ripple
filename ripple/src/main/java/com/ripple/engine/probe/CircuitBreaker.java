package com.ripple.engine.probe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Hand-rolled per-service circuit breaker state machine.
 *
 * <p><strong>Thread safety:</strong> all public methods are safe to call concurrently
 * from multiple virtual threads. State transitions use {@link AtomicReference#compareAndSet}
 * — no locking. The failure counter uses {@link AtomicInteger}.
 *
 * <p><strong>Why no Resilience4j:</strong> DDL-001. The state machine, atomic transitions,
 * and half-open probing logic must be visible in the codebase as a portfolio artefact.
 *
 * <p>State transitions:
 * <pre>
 * CLOSED    ──(N consecutive failures)──► OPEN
 * OPEN      ──(cooldown elapsed)────────► HALF_OPEN
 * HALF_OPEN ──(probe succeeds)───────────► CLOSED
 * HALF_OPEN ──(probe fails)──────────────► OPEN
 * </pre>
 *
 * <p>The {@code onStateChange} callback is invoked on every successful CAS transition.
 * Unit tests that are not testing callback behaviour pass a no-op: {@code state -> {}}.
 */
public final class CircuitBreaker implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(CircuitBreaker.class);

    private final CircuitBreakerConfig config;
    private final Consumer<CircuitBreakerState> onStateChange;

    // guarded by CAS — no locking
    private final AtomicReference<CircuitBreakerState> state =
        new AtomicReference<>(CircuitBreakerState.CLOSED);

    // consecutive failure counter; reset to 0 on CLOSED transition
    private final AtomicInteger failureCount = new AtomicInteger(0);

    // single virtual-thread scheduler for the cooldown timer
    private final ScheduledExecutorService cooldownScheduler =
        Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("circuit-breaker-cooldown").factory()
        );

    /**
     * Creates a circuit breaker with the given configuration and state-change callback.
     *
     * @param config         configuration (failure threshold, cooldown duration)
     * @param onStateChange  invoked on every state transition; use {@code state -> {}} in tests
     *                       that are not verifying the callback
     */
    public CircuitBreaker(CircuitBreakerConfig config, Consumer<CircuitBreakerState> onStateChange) {
        this.config = config;
        this.onStateChange = onStateChange;
    }

    /**
     * Records a probe failure. Transitions to {@code OPEN} if the failure threshold is reached.
     * In {@code HALF_OPEN}, any failure immediately transitions back to {@code OPEN}.
     */
    public void recordFailure() {
        CircuitBreakerState current = state.get();

        if (current == CircuitBreakerState.OPEN) {
            return; // already open — nothing to do
        }

        if (current == CircuitBreakerState.HALF_OPEN) {
            // Failed probe during half-open window → immediately back to OPEN
            transitionTo(CircuitBreakerState.HALF_OPEN, CircuitBreakerState.OPEN);
            return;
        }

        // CLOSED: increment consecutive failure counter
        int failures = failureCount.incrementAndGet();
        if (failures >= config.failureThreshold()) {
            transitionTo(CircuitBreakerState.CLOSED, CircuitBreakerState.OPEN);
        }
    }

    /**
     * Records a probe success. Transitions {@code HALF_OPEN → CLOSED} and resets the
     * failure counter. Has no effect in {@code CLOSED} or {@code OPEN} state.
     */
    public void recordSuccess() {
        CircuitBreakerState current = state.get();

        if (current == CircuitBreakerState.HALF_OPEN) {
            transitionTo(CircuitBreakerState.HALF_OPEN, CircuitBreakerState.CLOSED);
        } else if (current == CircuitBreakerState.CLOSED) {
            // Reset consecutive counter on any success in CLOSED state
            failureCount.set(0);
        }
    }

    /** Returns the current circuit breaker state. Safe to call from any thread. */
    public CircuitBreakerState state() {
        return state.get();
    }

    /** Returns the current consecutive failure count. Intended for metrics/health endpoints. */
    public int failureCount() {
        return failureCount.get();
    }

    /**
     * Attempts a CAS transition from {@code expected} to {@code next}.
     * If the CAS succeeds, fires the {@code onStateChange} callback and schedules
     * the cooldown timer if transitioning to {@code OPEN}.
     */
    private void transitionTo(CircuitBreakerState expected, CircuitBreakerState next) {
        if (state.compareAndSet(expected, next)) {
            log.warn("Circuit breaker transitioned {} → {}", expected, next);
            onStateChange.accept(next);

            if (next == CircuitBreakerState.OPEN) {
                failureCount.set(0); // reset so half-open re-entry starts clean
                scheduleCooldown();
            } else if (next == CircuitBreakerState.CLOSED) {
                failureCount.set(0);
            }
        }
    }

    /** Schedules the OPEN → HALF_OPEN transition after the configured cooldown. */
    private void scheduleCooldown() {
        cooldownScheduler.schedule(() -> {
            if (state.get() == CircuitBreakerState.OPEN) {
                transitionTo(CircuitBreakerState.OPEN, CircuitBreakerState.HALF_OPEN);
            }
        }, config.cooldownDuration().toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        cooldownScheduler.shutdownNow();
    }
}
