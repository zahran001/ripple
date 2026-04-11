package com.ripple.model;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

/**
 * Immutable result of a blast radius computation.
 *
 * <p>{@code isComplete} is {@code false} when the topology snapshot contains edges
 * pointing to nodes that have been evicted — the affected set is best-effort in this case.
 * The list of {@code unresolvedEdges} identifies the gap.
 */
public record BlastRadiusResult(
    ServiceId failedService,
    AffectedSet affected,
    Duration computedIn,
    Instant snapshotTimestamp,
    boolean isComplete,
    List<String> unresolvedEdges
) {

    /** Creates a complete (no unresolved edges) blast radius result. */
    public static BlastRadiusResult complete(ServiceId failed, AffectedSet affected,
                                             Duration computedIn, Instant snapshotTs) {
        return new BlastRadiusResult(failed, affected, computedIn, snapshotTs, true, List.of());
    }

    /** Creates a partial blast radius result (some edges unresolved). */
    public static BlastRadiusResult partial(ServiceId failed, AffectedSet affected,
                                            Duration computedIn, Instant snapshotTs,
                                            List<String> unresolvedEdges) {
        return new BlastRadiusResult(failed, affected, computedIn, snapshotTs, false, unresolvedEdges);
    }
}
