package com.ripple.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Configuration properties for the probe engine.
 * Bound from {@code ripple.probe.*} in {@code application.yml}.
 */
@ConfigurationProperties(prefix = "ripple.probe")
public record ProbeProperties(
    int concurrencyLimit,
    long defaultIntervalMs,
    long httpTimeoutMs,
    long tcpTimeoutMs,
    long defaultLatencyThresholdMs
) {
    public ProbeProperties {
        if (concurrencyLimit <= 0) concurrencyLimit = 50;
        if (defaultIntervalMs <= 0) defaultIntervalMs = 5000;
        if (httpTimeoutMs <= 0) httpTimeoutMs = 2000;
        if (tcpTimeoutMs <= 0) tcpTimeoutMs = 1000;
        if (defaultLatencyThresholdMs <= 0) defaultLatencyThresholdMs = 500;
    }
}
