package com.ripple.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for per-service circuit breakers.
 * Bound from {@code ripple.circuit-breaker.*} in {@code application.yml}.
 */
@ConfigurationProperties(prefix = "ripple.circuit-breaker")
public record CircuitBreakerProperties(
    int failureThreshold,
    long cooldownMs
) {
    public CircuitBreakerProperties {
        if (failureThreshold <= 0) failureThreshold = 5;
        if (cooldownMs <= 0) cooldownMs = 30000;
    }
}
