package com.ripple.engine.stream;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Monitors per-subscriber lag and sheds events when lag exceeds the high-water mark.
 *
 * <p><strong>Lag calculation:</strong> {@code lag = producedCount - subscriberConsumedCount}<br>
 * This uses Ripple's own counters rather than Chronicle Queue's internal index arithmetic,
 * which encodes cycle/date bits into the index value and is not directly subtractable.
 *
 * <p><strong>Shedding:</strong> When a subscriber's lag exceeds its {@code highWaterMark()},
 * the monitor advances that subscriber's tailer to the current tail of the queue.
 * This skips the backlogged events for that subscriber only — other subscribers are
 * completely unaffected. The underlying Chronicle Queue file is untouched.
 *
 * <p>Shedding emits a {@code subscriber.lagging} Micrometer counter per occurrence so
 * it can be monitored in Grafana.
 *
 * <p><strong>Policy by subscriber:</strong>
 * <ul>
 *   <li>{@code AlertRouter}, {@code StateStore}: {@code Long.MAX_VALUE} — never shed.</li>
 *   <li>{@code SseFeed}: low (100) — shed early; client recovers via snapshot fetch.</li>
 *   <li>{@code DegradationPlanner}: medium (1000) — shed acceptable, best-effort.</li>
 * </ul>
 */
@Component
public final class BackpressureMonitor {

    private static final Logger log = LoggerFactory.getLogger(BackpressureMonitor.class);

    private final EventBus eventBus;
    private final MeterRegistry meterRegistry;

    public BackpressureMonitor(EventBus eventBus, MeterRegistry meterRegistry) {
        this.eventBus = eventBus;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Checks lag for all registered subscribers and sheds where necessary.
     * Runs on a fixed schedule driven by {@code ripple.stream.backpressure-check-interval-ms}.
     */
    @Scheduled(fixedDelayString = "${ripple.stream.backpressure-check-interval-ms:100}")
    public void checkAndShed() {
        long produced = eventBus.producedCount();

        for (EventBus.SubscriberHandle handle : eventBus.handles()) {
            Subscriber subscriber = handle.subscriber();
            long consumed = handle.consumed().get();
            long lag = produced - consumed;

            // Record per-subscriber lag as a gauge
            meterRegistry.gauge("subscriber.lag",
                java.util.List.of(io.micrometer.core.instrument.Tag.of("subscriber", subscriber.name())),
                handle.consumed(), c -> produced - c.get());

            if (lag > subscriber.highWaterMark()) {
                shed(handle, produced, lag);
            }
        }
    }

    /**
     * Sheds events for the given subscriber by advancing its tailer to the queue tail.
     * Increments the {@code subscriber.lagging} metric.
     */
    private void shed(EventBus.SubscriberHandle handle, long producedCount, long lag) {
        Subscriber subscriber = handle.subscriber();
        log.warn("SUBSCRIBER_LAGGING [{}] — lag={}, high-water-mark={} — shedding",
            subscriber.name(), lag, subscriber.highWaterMark());

        // Advance tailer to current tail — skips all backlogged events
        handle.tailer().toEnd();
        // Sync consumed counter to match produced so lag resets to 0
        handle.consumed().set(producedCount);

        meterRegistry.counter("subscriber.lagging",
            "subscriber", subscriber.name()).increment();
    }

    /**
     * Returns the current lag for the given subscriber name. Intended for
     * the {@code /health} endpoint. Returns -1 if the subscriber is not registered.
     */
    public long lagFor(String subscriberName) {
        long produced = eventBus.producedCount();
        return eventBus.handles().stream()
            .filter(h -> h.subscriber().name().equals(subscriberName))
            .mapToLong(h -> produced - h.consumed().get())
            .findFirst()
            .orElse(-1L);
    }
}
