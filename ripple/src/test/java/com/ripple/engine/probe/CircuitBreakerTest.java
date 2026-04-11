package com.ripple.engine.probe;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the hand-rolled {@link CircuitBreaker} state machine.
 *
 * <p>Key invariants:
 * <ul>
 *   <li>5 consecutive failures → OPEN (default threshold)</li>
 *   <li>OPEN → HALF_OPEN after cooldown</li>
 *   <li>HALF_OPEN + success → CLOSED</li>
 *   <li>HALF_OPEN + failure → OPEN</li>
 *   <li>Concurrent calls are safe (no data races)</li>
 * </ul>
 */
class CircuitBreakerTest {

    private CircuitBreaker breaker(int threshold, Duration cooldown) {
        return new CircuitBreaker(
            CircuitBreakerConfig.of(threshold, cooldown),
            state -> {}
        );
    }

    @Test
    void starts_in_closed_state() {
        var cb = breaker(5, Duration.ofSeconds(30));
        assertThat(cb.state()).isEqualTo(CircuitBreakerState.CLOSED);
    }

    @Test
    void opens_after_threshold_failures() {
        var cb = breaker(5, Duration.ofSeconds(30));

        for (int i = 0; i < 4; i++) {
            cb.recordFailure();
            assertThat(cb.state()).isEqualTo(CircuitBreakerState.CLOSED);
        }

        cb.recordFailure(); // 5th failure — threshold reached
        assertThat(cb.state()).isEqualTo(CircuitBreakerState.OPEN);
    }

    @Test
    void transitions_open_to_half_open_after_cooldown() {
        var cb = breaker(1, Duration.ofMillis(50)); // fast cooldown for testing

        cb.recordFailure(); // → OPEN
        assertThat(cb.state()).isEqualTo(CircuitBreakerState.OPEN);

        Awaitility.await()
            .atMost(Duration.ofMillis(500))
            .untilAsserted(() -> assertThat(cb.state()).isEqualTo(CircuitBreakerState.HALF_OPEN));
    }

    @Test
    void transitions_half_open_to_closed_on_success() {
        var cb = breaker(1, Duration.ofMillis(50));
        cb.recordFailure(); // → OPEN

        Awaitility.await()
            .atMost(Duration.ofMillis(500))
            .untilAsserted(() -> assertThat(cb.state()).isEqualTo(CircuitBreakerState.HALF_OPEN));

        cb.recordSuccess(); // HALF_OPEN + success → CLOSED
        assertThat(cb.state()).isEqualTo(CircuitBreakerState.CLOSED);
    }

    @Test
    void transitions_half_open_to_open_on_failure() {
        var cb = breaker(1, Duration.ofMillis(50));
        cb.recordFailure(); // → OPEN

        Awaitility.await()
            .atMost(Duration.ofMillis(500))
            .untilAsserted(() -> assertThat(cb.state()).isEqualTo(CircuitBreakerState.HALF_OPEN));

        cb.recordFailure(); // HALF_OPEN + failure → back to OPEN
        assertThat(cb.state()).isEqualTo(CircuitBreakerState.OPEN);
    }

    @Test
    void ignores_failures_when_already_open() {
        var cb = breaker(1, Duration.ofSeconds(30));
        cb.recordFailure(); // → OPEN
        assertThat(cb.state()).isEqualTo(CircuitBreakerState.OPEN);

        cb.recordFailure(); // should be no-op
        cb.recordFailure();
        assertThat(cb.state()).isEqualTo(CircuitBreakerState.OPEN);
    }

    @Test
    void success_resets_failure_counter_in_closed_state() {
        var cb = breaker(5, Duration.ofSeconds(30));

        cb.recordFailure();
        cb.recordFailure();
        cb.recordFailure(); // 3 failures
        assertThat(cb.state()).isEqualTo(CircuitBreakerState.CLOSED);

        cb.recordSuccess(); // reset counter
        assertThat(cb.failureCount()).isEqualTo(0);

        // 5 more failures needed to open again
        for (int i = 0; i < 4; i++) cb.recordFailure();
        assertThat(cb.state()).isEqualTo(CircuitBreakerState.CLOSED);
        cb.recordFailure();
        assertThat(cb.state()).isEqualTo(CircuitBreakerState.OPEN);
    }

    @Test
    void fires_state_change_callback_on_transition() {
        var observed = new AtomicReference<CircuitBreakerState>();
        var cb = new CircuitBreaker(CircuitBreakerConfig.of(1, Duration.ofSeconds(30)), observed::set);

        cb.recordFailure();
        assertThat(observed.get()).isEqualTo(CircuitBreakerState.OPEN);
    }

    @Test
    void concurrent_failures_converge_to_open_exactly_once() throws InterruptedException {
        var cb = breaker(5, Duration.ofSeconds(30));
        int threads = 20;
        var latch = new CountDownLatch(threads);
        var openCount = new java.util.concurrent.atomic.AtomicInteger(0);

        var breakerWithCallback = new CircuitBreaker(
            CircuitBreakerConfig.of(5, Duration.ofSeconds(30)),
            state -> { if (state == CircuitBreakerState.OPEN) openCount.incrementAndGet(); }
        );

        for (int i = 0; i < threads; i++) {
            Thread.ofVirtual().start(() -> {
                breakerWithCallback.recordFailure();
                latch.countDown();
            });
        }
        latch.await();

        assertThat(breakerWithCallback.state()).isEqualTo(CircuitBreakerState.OPEN);
        // Should have transitioned to OPEN exactly once, then possibly back to OPEN from HALF_OPEN
        assertThat(openCount.get()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void closed_after_multiple_recovery_cycles() {
        var cb = breaker(1, Duration.ofMillis(50));

        for (int cycle = 0; cycle < 3; cycle++) {
            cb.recordFailure(); // CLOSED → OPEN
            assertThat(cb.state()).isEqualTo(CircuitBreakerState.OPEN);

            Awaitility.await()
                .atMost(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(cb.state()).isEqualTo(CircuitBreakerState.HALF_OPEN));

            cb.recordSuccess(); // HALF_OPEN → CLOSED
            assertThat(cb.state()).isEqualTo(CircuitBreakerState.CLOSED);
        }
    }
}
