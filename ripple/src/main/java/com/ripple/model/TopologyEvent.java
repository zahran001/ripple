package com.ripple.model;

import java.time.Instant;

/**
 * Structural mutation event emitted by {@code TopologyDiffEmitter} when the
 * topology graph changes between two consecutive snapshots.
 */
public record TopologyEvent(
    EventType type,
    ServiceId subject,
    ServiceId relatedNode,   // non-null for edge events; null for node events
    Instant timestamp,
    long snapshotVersion
) {

    public static TopologyEvent nodeAdded(ServiceId id, long version) {
        return new TopologyEvent(EventType.NODE_ADDED, id, null, Instant.now(), version);
    }

    public static TopologyEvent nodeRemoved(ServiceId id, long version) {
        return new TopologyEvent(EventType.NODE_REMOVED, id, null, Instant.now(), version);
    }

    public static TopologyEvent edgeAdded(ServiceId from, ServiceId to, long version) {
        return new TopologyEvent(EventType.EDGE_ADDED, from, to, Instant.now(), version);
    }

    public static TopologyEvent edgeRemoved(ServiceId from, ServiceId to, long version) {
        return new TopologyEvent(EventType.EDGE_REMOVED, from, to, Instant.now(), version);
    }

    public static TopologyEvent nodeEvicted(ServiceId id, long version) {
        return new TopologyEvent(EventType.NODE_EVICTED, id, null, Instant.now(), version);
    }
}
