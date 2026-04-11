package com.ripple.model;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Immutable point-in-time copy of the topology graph.
 *
 * <p>Used by the blast radius engine to run BFS without holding any locks on the
 * live {@code TopologyGraph}. Snapshots are passed by value — mutations after capture
 * do not affect this instance.
 *
 * <p>{@code version} monotonically increases with every structural mutation.
 * It is used by the SSE client to detect topology changes.
 */
public record GraphSnapshot(
    Map<ServiceId, Set<ServiceId>> adjacency,           // service → what it depends on
    Map<ServiceId, Set<ServiceId>> reverseAdjacency,    // service → what depends on it
    Map<ServiceId, ServiceNode> nodes,
    Instant capturedAt,
    long version
) {

    /**
     * Returns a deep-copy constructor ensuring no external mutation can affect this snapshot.
     */
    public GraphSnapshot {
        adjacency = deepCopyMap(adjacency);
        reverseAdjacency = deepCopyMap(reverseAdjacency);
        nodes = Collections.unmodifiableMap(new HashMap<>(nodes));
    }

    public boolean hasNode(ServiceId id) {
        return nodes.containsKey(id);
    }

    public Set<ServiceId> dependentsOf(ServiceId id) {
        return reverseAdjacency.getOrDefault(id, Set.of());
    }

    public Set<ServiceId> dependenciesOf(ServiceId id) {
        return adjacency.getOrDefault(id, Set.of());
    }

    public ServiceNode nodeFor(ServiceId id) {
        return nodes.get(id);
    }

    private static Map<ServiceId, Set<ServiceId>> deepCopyMap(Map<ServiceId, Set<ServiceId>> src) {
        var copy = new HashMap<ServiceId, Set<ServiceId>>();
        src.forEach((k, v) -> copy.put(k, Collections.unmodifiableSet(new HashSet<>(v))));
        return Collections.unmodifiableMap(copy);
    }

    public static GraphSnapshot empty() {
        return new GraphSnapshot(Map.of(), Map.of(), Map.of(), Instant.now(), 0L);
    }
}
