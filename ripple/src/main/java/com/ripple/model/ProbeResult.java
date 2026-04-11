package com.ripple.model;

import java.time.Duration;
import java.time.Instant;

/**
 * Immutable result of a single probe attempt against a service.
 *
 * <p>Every probe attempt — regardless of outcome — produces a {@code ProbeResult}.
 * This ensures the event stream always has a complete picture of probe activity,
 * not just failures.
 */
public record ProbeResult(
    ServiceId serviceId,
    ProbeStatus status,
    Duration latency,
    Instant timestamp,
    String detail
) {

    /** Factory for a successful probe result. */
    public static ProbeResult success(ServiceId id, Duration latency) {
        return new ProbeResult(id, ProbeStatus.SUCCESS, latency, Instant.now(), null);
    }

    /** Factory for a degraded probe result (reachable, but slow). */
    public static ProbeResult degraded(ServiceId id, Duration latency, String detail) {
        return new ProbeResult(id, ProbeStatus.DEGRADED, latency, Instant.now(), detail);
    }

    /** Factory for a failed probe result. */
    public static ProbeResult failure(ServiceId id, Duration latency, String detail) {
        return new ProbeResult(id, ProbeStatus.FAILURE, latency, Instant.now(), detail);
    }

    /** Factory for a circuit-open result (probe was skipped). */
    public static ProbeResult circuitOpen(ServiceId id, Instant timestamp) {
        return new ProbeResult(id, ProbeStatus.CIRCUIT_OPEN, Duration.ZERO, timestamp, "circuit breaker open");
    }
}
