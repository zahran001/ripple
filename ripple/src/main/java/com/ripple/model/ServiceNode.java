package com.ripple.model;

import java.net.URI;
import java.time.Duration;

/**
 * Immutable descriptor for a registered service.
 *
 * <p>{@code criticality} is an integer in [1, 10] declared by the service owner.
 * It encodes domain knowledge used by the blast radius scoring formula:
 * {@code score = criticality / (depth + 1)}.
 *
 * <p>{@code probeInterval} defaults to {@code ripple.probe.default-interval-ms} when
 * not specified per-service.
 */
public record ServiceNode(
    ServiceId id,
    String name,
    Protocol protocol,
    URI endpoint,
    int criticality,
    Duration probeInterval,
    long latencyThresholdMs
) {

    public ServiceNode {
        if (criticality < 1 || criticality > 10) {
            throw new IllegalArgumentException("criticality must be in [1, 10], got: " + criticality);
        }
    }

    /** Creates a node with default criticality (5) and provided probe interval. */
    public static ServiceNode of(ServiceId id, String name, Protocol protocol,
                                 URI endpoint, Duration probeInterval, long latencyThresholdMs) {
        return new ServiceNode(id, name, protocol, endpoint, 5, probeInterval, latencyThresholdMs);
    }
}
