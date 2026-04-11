package com.ripple.engine.stream;

import com.ripple.model.FailureEvent;

/**
 * Consumer of {@link FailureEvent} records from the event bus.
 *
 * <p>Each implementation declares its own {@link #highWaterMark()} based on what
 * missing an event actually means for that subscriber. This is not a shared constant —
 * the whole point is that different subscribers have different tolerances.
 *
 * <p>Tolerance rationale:
 * <ul>
 *   <li>{@code AlertRouter} — {@link Long#MAX_VALUE}. A missed event is a missed
 *       incident alert. An on-call engineer does not get paged. Never shed.</li>
 *   <li>{@code StateStore} — {@link Long#MAX_VALUE}. A missed event is a gap in the
 *       historical record. Replay and audit are broken. Never shed.</li>
 *   <li>{@code SseFeed} — low value (e.g., 100). The dashboard recovers via a full
 *       snapshot fetch when a sequence number gap is detected. Safe to shed early.</li>
 *   <li>{@code DegradationPlanner} — medium value (e.g., 1000). Runbook suggestions
 *       are best-effort — not critical path. Shedding is acceptable.</li>
 * </ul>
 *
 * @see com.ripple.engine.stream.BackpressureMonitor
 */
public interface Subscriber {

    /** Called for each event delivered from the event bus. Must not throw. */
    void onEvent(FailureEvent event);

    /**
     * Returns the maximum number of events this subscriber can lag behind the producer
     * before the {@link BackpressureMonitor} begins shedding events for it.
     *
     * <p>Implementations that cannot tolerate missing events ({@code AlertRouter},
     * {@code StateStore}) must return {@link Long#MAX_VALUE}.
     * Implementations with a recovery path ({@code SseFeed}) should return a low value
     * to shed early and recover fast via snapshot fetch.
     */
    long highWaterMark();

    /** Human-readable name for logging and metrics. */
    String name();
}
