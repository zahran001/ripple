package com.ripple.model;

/**
 * Classification of a failure event in the event stream.
 */
public enum EventType {

    /** A service probe returned FAILURE. */
    SERVICE_FAILURE,

    /** A service probe returned DEGRADED. */
    SERVICE_DEGRADED,

    /** A previously failed service probe returned SUCCESS. */
    SERVICE_RECOVERED,

    /** A circuit breaker transitioned to OPEN. */
    CIRCUIT_OPENED,

    /** A circuit breaker transitioned to HALF_OPEN. */
    CIRCUIT_HALF_OPENED,

    /** A circuit breaker transitioned back to CLOSED. */
    CIRCUIT_CLOSED,

    /** A new service node was added to the topology. */
    NODE_ADDED,

    /** A service node was removed from the topology. */
    NODE_REMOVED,

    /** A dependency edge was added to the topology. */
    EDGE_ADDED,

    /** A dependency edge was removed from the topology. */
    EDGE_REMOVED,

    /** A stale service node was evicted by the TTL sweep. */
    NODE_EVICTED
}
