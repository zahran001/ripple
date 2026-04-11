package com.ripple.engine.probe;

import com.ripple.model.ProbeResult;
import com.ripple.model.ServiceNode;

/**
 * Strategy interface for performing a health-check probe against a single service.
 *
 * <p>Each implementation handles one protocol: HTTP, gRPC, TCP, or script.
 * Implementations must be thread-safe — they are called concurrently from virtual threads.
 *
 * <p>Implementations return a {@link ProbeResult} on both success and expected failures.
 * Unexpected errors (e.g., network stack errors) should be caught and returned as
 * {@link com.ripple.model.ProbeStatus#FAILURE} results — never re-thrown.
 */
@FunctionalInterface
public interface Prober {

    /**
     * Executes a probe against the given service node.
     *
     * <p>The latency measurement (for DEGRADED detection) is performed by
     * {@link ProbeScheduler} — implementations do not need to measure it themselves.
     *
     * @param node the service to probe
     * @return a non-null probe result; never throws
     */
    ProbeResult probe(ServiceNode node);
}
