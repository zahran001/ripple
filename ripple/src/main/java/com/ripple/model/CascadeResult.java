package com.ripple.model;

import java.util.Map;

/**
 * Result of a cascade failure simulation.
 *
 * <p>Extends {@link BlastRadiusResult} with per-service {@link FailureMode} assignments
 * and the wave number in which each service was first reached by the cascade propagation.
 *
 * <p>Wave semantics:
 * <ul>
 *   <li>Wave 0 — the root node (always {@code HARD_DOWN})</li>
 *   <li>Wave 1 — direct dependents that exceeded the cascade threshold</li>
 *   <li>Wave N — dependents of wave N-1 nodes that exceeded the threshold</li>
 * </ul>
 */
public record CascadeResult(
    BlastRadiusResult blastRadius,
    Map<ServiceId, FailureMode> failureModes,
    Map<ServiceId, Integer> waveAssignments,
    double cascadeThreshold,
    int totalWaves
) {

    public FailureMode modeOf(ServiceId id) {
        return failureModes.getOrDefault(id, null);
    }

    public int waveOf(ServiceId id) {
        return waveAssignments.getOrDefault(id, -1);
    }
}
