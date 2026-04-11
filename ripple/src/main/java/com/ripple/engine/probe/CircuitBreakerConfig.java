package com.ripple.engine.probe;

import com.ripple.config.CircuitBreakerProperties;

import java.time.Duration;

/**
 * Immutable configuration for a single {@link CircuitBreaker} instance.
 *
 * <p>Per-service configs can override the defaults from {@link CircuitBreakerProperties}.
 */
public record CircuitBreakerConfig(
    int failureThreshold,
    Duration cooldownDuration
) {

    /** Default config with 5 failure threshold and 30s cooldown. */
    public static CircuitBreakerConfig defaults() {
        return new CircuitBreakerConfig(5, Duration.ofSeconds(30));
    }

    /** Config with a custom cooldown — useful for testing. */
    public static CircuitBreakerConfig withCooldown(Duration cooldown) {
        return new CircuitBreakerConfig(5, cooldown);
    }

    /** Config with a custom failure threshold and cooldown. */
    public static CircuitBreakerConfig of(int failureThreshold, Duration cooldown) {
        return new CircuitBreakerConfig(failureThreshold, cooldown);
    }

    /** Create from Spring properties. */
    public static CircuitBreakerConfig from(CircuitBreakerProperties props) {
        return new CircuitBreakerConfig(
            props.failureThreshold(),
            Duration.ofMillis(props.cooldownMs())
        );
    }
}
