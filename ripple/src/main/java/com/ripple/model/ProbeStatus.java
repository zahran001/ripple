package com.ripple.model;

/**
 * Result status of a single probe attempt.
 *
 * <p>The distinction between {@code DEGRADED} and {@code FAILURE} is first-class:
 * a service that responds slowly is meaningfully different from one that does not respond
 * at all. The blast radius engine propagates this distinction into severity scores.
 */
public enum ProbeStatus {

    /** Service responded within its configured latency threshold. */
    SUCCESS,

    /** Service responded but exceeded the configured latency threshold. */
    DEGRADED,

    /** Service did not respond or returned an error code. */
    FAILURE,

    /** Probe was skipped because this service's circuit breaker is OPEN. */
    CIRCUIT_OPEN
}
