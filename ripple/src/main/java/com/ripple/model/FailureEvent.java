package com.ripple.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/**
 * A failure or topology event published to the event bus and fanned out to all subscribers.
 *
 * <p>{@code eventIndex} — the Chronicle Queue tailer index stamped at append time by
 * {@code EventBus}. This value is carried as the SSE {@code id:} field so the dashboard
 * client can detect shedding gaps and trigger the snapshot-based recovery path.
 * The field is {@code -1} until the event is appended to the queue.
 *
 * <p>{@code blastRadius} may be {@code null} for topology events (node add/remove)
 * that do not result from a probe failure.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record FailureEvent(
    EventType type,
    ServiceId origin,
    BlastRadiusResult blastRadius,   // null for topology-only events
    Instant timestamp,
    long eventIndex                  // Chronicle Queue tailer index; carried as SSE id:
) {

    /** Creates an event without a blast radius (topology events). */
    public static FailureEvent topology(EventType type, ServiceId origin) {
        return new FailureEvent(type, origin, null, Instant.now(), -1L);
    }

    /** Creates a failure event with a blast radius result. */
    public static FailureEvent failure(EventType type, ServiceId origin, BlastRadiusResult blastRadius) {
        return new FailureEvent(type, origin, blastRadius, Instant.now(), -1L);
    }

    /** Returns a copy of this event with the Chronicle Queue index stamped. */
    public FailureEvent withEventIndex(long index) {
        return new FailureEvent(type, origin, blastRadius, timestamp, index);
    }
}
