package com.ripple.model;

/**
 * Describes how a service was affected in a cascade simulation.
 */
public enum FailureMode {

    /** The root node that was directly failed or hypothetically failed. */
    HARD_DOWN,

    /** The service is reachable but slow — derived from ProbeStatus.DEGRADED. */
    DEGRADED,

    /**
     * The service was not directly failed but cascaded as a result of its
     * dependencies exceeding the configurable cascade threshold.
     */
    CASCADED
}
