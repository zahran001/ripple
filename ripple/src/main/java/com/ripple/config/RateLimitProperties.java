package com.ripple.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the token-bucket rate limiter.
 * Bound from {@code ripple.rate-limit.*} in {@code application.yml}.
 *
 * <p>{@code maxBurst} caps token accumulation during idle periods so a quiet caller
 * cannot build up credit and fire a burst larger than the cap. Default: 2× sustained rate.
 */
@ConfigurationProperties(prefix = "ripple.rate-limit")
public record RateLimitProperties(
    long requestsPerSecond,
    long maxBurst
) {
    public RateLimitProperties {
        if (requestsPerSecond <= 0) requestsPerSecond = 100;
        if (maxBurst <= 0) maxBurst = requestsPerSecond * 2;
    }
}
