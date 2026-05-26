package com.ripple.api;

import com.ripple.config.RateLimitProperties;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the token-bucket rate limiter.
 *
 * <p>Key invariants:
 * <ul>
 *   <li>First {@code maxBurst} requests succeed immediately</li>
 *   <li>Requests beyond {@code maxBurst} are rejected until tokens refill</li>
 *   <li>Different caller IDs have independent buckets</li>
 *   <li>Tokens refill over time at the configured sustained rate</li>
 *   <li>Idle bucket does not accumulate beyond {@code maxBurst} (the cap)</li>
 * </ul>
 */
class TokenBucketRateLimiterTest {

    private TokenBucketRateLimiter rateLimiter(long rps, long maxBurst) {
        return new TokenBucketRateLimiter(new RateLimitProperties(rps, maxBurst));
    }

    @Test
    void first_maxBurst_requests_succeed() {
        var limiter = rateLimiter(100, 5);

        for (int i = 0; i < 5; i++) {
            assertThat(limiter.tryAcquire("caller-A")).isTrue();
        }
    }

    @Test
    void requests_beyond_maxBurst_are_rejected() {
        var limiter = rateLimiter(100, 5);

        for (int i = 0; i < 5; i++) limiter.tryAcquire("caller-B"); // drain the bucket

        assertThat(limiter.tryAcquire("caller-B")).isFalse(); // 6th request rejected
    }

    @Test
    void different_callers_have_independent_buckets() {
        var limiter = rateLimiter(100, 3);

        // Drain caller-X bucket
        limiter.tryAcquire("caller-X");
        limiter.tryAcquire("caller-X");
        limiter.tryAcquire("caller-X");
        assertThat(limiter.tryAcquire("caller-X")).isFalse(); // X bucket empty

        // caller-Y bucket should still be full
        assertThat(limiter.tryAcquire("caller-Y")).isTrue();
    }

    @Test
    void tokens_refill_over_time() throws InterruptedException {
        // 1000 rps → 1 token per ms
        var limiter = rateLimiter(1000, 5);

        for (int i = 0; i < 5; i++) limiter.tryAcquire("refill-caller");
        assertThat(limiter.tryAcquire("refill-caller")).isFalse(); // bucket empty

        Thread.sleep(10); // wait ~10ms → ~10 tokens refilled, capped at maxBurst=5

        int accepted = 0;
        for (int i = 0; i < 5; i++) {
            if (limiter.tryAcquire("refill-caller")) accepted++;
        }
        assertThat(accepted).isGreaterThanOrEqualTo(4); // bucket should refill to maxBurst=5
    }

    @Test
    void idle_bucket_does_not_exceed_maxBurst() throws InterruptedException {
        long maxBurst = 5;
        var limiter = rateLimiter(10, maxBurst);

        // Let the bucket be idle for a while — tokens should not accumulate beyond maxBurst
        Thread.sleep(1000); // 10 tokens would accumulate without cap; should be capped at 5

        int accepted = 0;
        for (int i = 0; i < 20; i++) {
            if (limiter.tryAcquire("idle-caller")) accepted++;
        }

        // Accepted should be at most maxBurst (5) — not 10 (uncapped refill)
        assertThat(accepted).isLessThanOrEqualTo((int) maxBurst);
    }

    @Test
    void concurrent_callers_do_not_exceed_burst_limit() throws InterruptedException {
        long maxBurst = 10;
        var limiter = rateLimiter(100, maxBurst);

        int threads = 50;
        var accepted = new AtomicInteger(0);
        var latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            Thread.ofVirtual().start(() -> {
                if (limiter.tryAcquire("concurrent-caller")) accepted.incrementAndGet();
                latch.countDown();
            });
        }
        latch.await();

        // Exactly maxBurst threads should succeed — CAS loop is correct so no over-counting
        assertThat(accepted.get()).isEqualTo((int) maxBurst);
    }
}
