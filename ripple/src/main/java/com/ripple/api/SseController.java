package com.ripple.api;

import com.ripple.engine.stream.SseSubscriber;
import com.ripple.model.FailureEvent;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/**
 * Server-Sent Events endpoint that streams {@link FailureEvent} records to connected clients.
 *
 * <p><strong>SSE {@code id:} field:</strong> Every event carries {@link FailureEvent#eventIndex()}
 * as the SSE {@code id:} field — the Chronicle Queue tailer index stamped at append time (DDL-014).
 * This is non-optional. The dashboard client tracks the last received {@code id:} value.
 * When {@code currentId > previousId + 1}, a gap is detected (events were shed due to backpressure)
 * and the client triggers the recovery sequence: fetch {@code GET /topology}, re-render, resume.
 *
 * <p><strong>Why WebFlux here:</strong> SSE is a long-lived streaming response. Blocking a
 * virtual thread for the duration of the connection is wasteful (the thread is idle 99.9% of
 * the time). {@link Flux} here means only the event delivery is non-blocking; the rest of the
 * application remains on Spring MVC with virtual threads.
 *
 * <p>The {@code SseSubscriber} is a {@link com.ripple.engine.stream.Subscriber} registered with
 * the {@link com.ripple.engine.stream.EventBus}. It bridges from the Chronicle drain virtual
 * thread to this reactive pipeline via a {@link reactor.core.publisher.Sinks.Many}.
 */
@RestController
@RequestMapping("/events")
public class SseController {

    private final SseSubscriber sseSubscriber;

    public SseController(SseSubscriber sseSubscriber) {
        this.sseSubscriber = sseSubscriber;
    }

    /**
     * Streams live failure events to the client as Server-Sent Events.
     *
     * <p>Each SSE message has:
     * <ul>
     *   <li>{@code id:} — the Chronicle Queue event index for gap detection</li>
     *   <li>{@code event:} — the {@link com.ripple.model.EventType} name</li>
     *   <li>{@code data:} — JSON-serialized {@link FailureEvent}</li>
     * </ul>
     *
     * <p>Client recovery path: on detecting {@code currentId > lastId + 1}:
     * <ol>
     *   <li>Fetch {@code GET /topology} to get the current full snapshot</li>
     *   <li>Re-render the dependency graph from the snapshot</li>
     *   <li>Resume SSE stream from the current connection</li>
     * </ol>
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<FailureEvent>> streamEvents() {
        return sseSubscriber.asFlux()
            .map(event -> ServerSentEvent.<FailureEvent>builder()
                .id(String.valueOf(event.eventIndex()))    // Chronicle index — enables gap detection
                .event(event.type().name())                // EventType name for client-side filtering
                .data(event)
                .build()
            );
    }
}
