package com.ripple.engine.stream;

import com.ripple.model.FailureEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Sinks;

/**
 * Bridges the event bus to the WebFlux SSE stream.
 *
 * <p>Each event delivered via {@link #onEvent} is emitted into a {@link Sinks.Many} hot sink.
 * {@code SseController} subscribes to this sink and pushes events to connected dashboard clients
 * as {@code ServerSentEvent} records carrying the {@code eventIndex} in the SSE {@code id:} field.
 *
 * <p><strong>SSE {@code id:} field specification (DDL-014):</strong> The {@code id:} field
 * carries {@link FailureEvent#eventIndex()} — the Chronicle Queue tailer index stamped at
 * append time. The dashboard client tracks the last received {@code id:} value. When
 * {@code currentId > previousId + 1}, a gap is detected and the client triggers the recovery
 * sequence: fetch {@code GET /topology}, re-render, resume live stream.
 * <strong>This field is non-optional.</strong> Omitting it silently disables the recovery path.
 *
 * <p><strong>Backpressure tolerance:</strong> Low ({@code 100}). The dashboard recovers via a
 * full snapshot fetch when a sequence number gap is detected on the client side. Shedding early
 * keeps the event bus flowing — an observability tool that freezes its own dashboard under load
 * defeats its own purpose.
 *
 * <p>Thread-safe — {@link Sinks.Many} is designed for concurrent emission.
 */
@Component
public final class SseSubscriber implements Subscriber {

    private static final Logger log = LoggerFactory.getLogger(SseSubscriber.class);

    private static final long SSE_HIGH_WATER_MARK = 100L;

    // Hot sink — all SSE subscribers share one event stream
    private final Sinks.Many<FailureEvent> sink = Sinks.many().multicast().onBackpressureBuffer(256);

    @Override
    public void onEvent(FailureEvent event) {
        Sinks.EmitResult result = sink.tryEmitNext(event);
        if (result.isFailure()) {
            log.warn("SseSubscriber: failed to emit event [{}] to sink — result: {}", event.type(), result);
        }
    }

    @Override
    public long highWaterMark() {
        return SSE_HIGH_WATER_MARK; // shed early; client recovers via GET /topology
    }

    @Override
    public String name() {
        return "sse-feed";
    }

    /**
     * Returns the Flux that {@code SseController} subscribes to.
     * The Flux is hot — it only delivers events emitted after subscription.
     */
    public reactor.core.publisher.Flux<FailureEvent> asFlux() {
        return sink.asFlux();
    }
}
