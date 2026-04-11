package com.ripple.api;

import com.ripple.config.RateLimitProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lock-free token bucket rate limiter using an {@link AtomicLong} CAS loop.
 *
 * <p><strong>Why no Bucket4j/Guava RateLimiter:</strong> DDL-007. The token bucket
 * algorithm is a core backend engineering concept and the implementation must be visible.
 * {@code AtomicLong} CAS gives us a lock-free, non-blocking implementation.
 *
 * <p><strong>Algorithm — lazy refill:</strong> Rather than a scheduled refill thread
 * (which introduces clock drift), tokens are computed lazily on each {@code tryAcquire}
 * call using wall-clock time. This eliminates the scheduler entirely and is drift-free.
 * {@code tokensToAdd = elapsedNanos × ratePerNano}, capped at {@code maxBurst}.
 *
 * <p><strong>{@code maxBurst}:</strong> Caps token accumulation during idle periods.
 * Without this cap, a quiet caller could accumulate unlimited tokens and fire a burst
 * far larger than the configured sustained rate. Default: 2× {@code requestsPerSecond}.
 *
 * <p>Per-caller buckets are maintained in a {@link ConcurrentHashMap}, keyed by caller ID
 * (API key or IP address). Buckets are created lazily on first access.
 *
 * <p>Thread-safe — all state mutations go through CAS loops on {@link AtomicLong}.
 */
@Component
public final class TokenBucketRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(TokenBucketRateLimiter.class);

    private final double ratePerNano;   // tokens per nanosecond
    private final long maxBurst;        // token ceiling — caps idle accumulation

    // per-caller buckets, created lazily
    private final ConcurrentHashMap<String, CallerBucket> buckets = new ConcurrentHashMap<>();

    public TokenBucketRateLimiter(RateLimitProperties props) {
        this.ratePerNano = (double) props.requestsPerSecond() / 1_000_000_000L;
        this.maxBurst = props.maxBurst();
        log.info("TokenBucketRateLimiter: {}req/s, maxBurst={}", props.requestsPerSecond(), maxBurst);
    }

    /**
     * Attempts to consume one token for the given caller.
     *
     * @param callerId  API key or IP address identifying the caller
     * @return {@code true} if a token was available and consumed; {@code false} if the
     *         bucket is exhausted (caller should receive 503 + Retry-After)
     */
    public boolean tryAcquire(String callerId) {
        CallerBucket bucket = buckets.computeIfAbsent(callerId, id ->
            new CallerBucket(maxBurst, System.nanoTime())
        );
        return bucket.tryConsume(ratePerNano, maxBurst);
    }

    /**
     * Returns how many nanoseconds until the next token becomes available for the caller.
     * Used to populate the {@code Retry-After} header.
     */
    public long nanosUntilNextToken(String callerId) {
        CallerBucket bucket = buckets.get(callerId);
        if (bucket == null) return 0L;
        long tokensNow = bucket.tokens.get();
        if (tokensNow > 0) return 0L;
        // Time until 1 token refills
        return (long)(1.0 / ratePerNano);
    }

    // =====================================================================
    // Per-caller bucket — holds token count and last refill timestamp
    // =====================================================================

    static final class CallerBucket {
        // Tokens stored as fixed-point long: actual tokens = tokenCount / SCALE
        // Simple approach: store as long nanoseconds of "token time"
        // or store as straight token count (simpler, slight precision loss acceptable)
        final AtomicLong tokens;
        final AtomicLong lastRefillNanos;

        CallerBucket(long initialTokens, long nowNanos) {
            this.tokens = new AtomicLong(initialTokens);
            this.lastRefillNanos = new AtomicLong(nowNanos);
        }

        /**
         * CAS-based lazy refill + token consumption.
         * Returns {@code true} if a token was consumed; {@code false} if bucket empty.
         */
        boolean tryConsume(double ratePerNano, long maxBurst) {
            while (true) {
                long now = System.nanoTime();
                long lastRefill = lastRefillNanos.get();
                long elapsed = now - lastRefill;
                long tokensToAdd = (long)(elapsed * ratePerNano);

                long currentTokens = tokens.get();
                long newTokens = Math.min(currentTokens + tokensToAdd, maxBurst);

                // Try to update refill time only if we have tokens to add
                if (tokensToAdd > 0) {
                    if (!lastRefillNanos.compareAndSet(lastRefill, now)) {
                        continue; // another thread updated refill time — retry
                    }
                    tokens.set(newTokens); // safe: we hold the refill timestamp CAS
                    currentTokens = newTokens;
                }

                if (currentTokens <= 0) {
                    return false; // bucket exhausted
                }

                // CAS: consume one token
                if (tokens.compareAndSet(currentTokens, currentTokens - 1)) {
                    return true;
                }
                // CAS failed — another thread consumed a token — retry
            }
        }
    }
}
