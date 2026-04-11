package com.ripple.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the failure event stream (Chronicle Queue + backpressure).
 * Bound from {@code ripple.stream.*} in {@code application.yml}.
 */
@ConfigurationProperties(prefix = "ripple.stream")
public record StreamProperties(
    String dataDir,
    long alertRouterHighWaterMark,
    long stateStoreHighWaterMark,
    long sseHighWaterMark,
    long degradationPlannerHighWaterMark,
    long backpressureCheckIntervalMs
) {
    public StreamProperties {
        if (dataDir == null || dataDir.isBlank()) dataDir = "/tmp/ripple-chronicle";
        if (alertRouterHighWaterMark <= 0) alertRouterHighWaterMark = Long.MAX_VALUE;
        if (stateStoreHighWaterMark <= 0) stateStoreHighWaterMark = Long.MAX_VALUE;
        if (sseHighWaterMark <= 0) sseHighWaterMark = 100;
        if (degradationPlannerHighWaterMark <= 0) degradationPlannerHighWaterMark = 1000;
        if (backpressureCheckIntervalMs <= 0) backpressureCheckIntervalMs = 100;
    }
}
