package com.ripple.engine.stream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ripple.model.FailureEvent;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Central fan-out hub backed by a Chronicle Queue off-heap ring buffer.
 *
 * <p><strong>Design:</strong>
 * <ul>
 *   <li>Producers append events to the queue tail via a single {@link ExcerptAppender}.</li>
 *   <li>Each subscriber has its own named {@link ExcerptTailer} — reads are independent.</li>
 *   <li>Each subscriber drains from its tailer on a dedicated virtual thread.</li>
 *   <li>The {@code eventIndex} field on {@link FailureEvent} is stamped with the
 *       Chronicle Queue tailer index at append time (DDL-014). This index becomes
 *       the SSE {@code id:} field for gap detection on the dashboard client.</li>
 * </ul>
 *
 * <p><strong>Backpressure:</strong> handled by {@link BackpressureMonitor}, which
 * monitors lag per subscriber and advances slow tailers past the high-water mark.
 *
 * <p><strong>Shedding semantics:</strong> Shedding advances the slow subscriber's tailer
 * to the current producer position — it does NOT delete events from the queue.
 * Other subscribers are completely unaffected.
 *
 * <p>Thread-safe. The appender is used from a single producer thread.
 * Subscriber drain threads each own their tailer exclusively.
 */
@Component
public final class EventBus {

    private static final Logger log = LoggerFactory.getLogger(EventBus.class);

    private final ChronicleQueue queue;
    private final ExcerptAppender appender;
    private final ObjectMapper objectMapper;
    private final List<SubscriberHandle> handles = new CopyOnWriteArrayList<>();

    // monotonic counter — events produced; used for lag calculation alongside Chronicle index
    private final AtomicLong produced = new AtomicLong(0);

    public EventBus(String dataDir, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.queue = SingleChronicleQueueBuilder.binary(dataDir).build();
        this.appender = queue.createAppender();
        log.info("EventBus started — Chronicle Queue at {}", dataDir);
    }

    /**
     * Appends a {@link FailureEvent} to the queue and returns the Chronicle index
     * stamped onto the event. The returned event carries the {@code eventIndex} field.
     */
    public FailureEvent publish(FailureEvent event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            appender.writeText(json);
            long index = appender.lastIndexAppended();
            long count = produced.incrementAndGet();

            FailureEvent stamped = event.withEventIndex(index);
            log.debug("EventBus published [{}] — index={} total={}", event.type(), index, count);
            return stamped;

        } catch (JsonProcessingException e) {
            log.error("EventBus: failed to serialize event [{}]: {}", event.type(), e.getMessage());
            return event; // return unstamped on serialization failure; subscribers will log
        }
    }

    /**
     * Registers a subscriber and starts its drain loop on a virtual thread.
     * The subscriber's named tailer starts at the current tail (live events only).
     */
    public void subscribe(Subscriber subscriber) {
        ExcerptTailer tailer = queue.createTailer(subscriber.name());
        tailer.toEnd(); // start from now — no historical replay on fresh subscription
        AtomicLong consumed = new AtomicLong(0);
        var handle = new SubscriberHandle(subscriber, tailer, consumed);
        handles.add(handle);

        Thread.ofVirtual()
            .name("subscriber-" + subscriber.name())
            .start(() -> drainLoop(handle));

        log.info("Subscriber [{}] registered — high water mark: {}", subscriber.name(), subscriber.highWaterMark());
    }

    /** Returns all active subscriber handles. Used by {@link BackpressureMonitor}. */
    public List<SubscriberHandle> handles() {
        return List.copyOf(handles);
    }

    /** Total events produced since startup. */
    public long producedCount() {
        return produced.get();
    }

    /**
     * Drain loop — runs on a dedicated virtual thread per subscriber.
     * Reads events from the tailer and delivers them to the subscriber.
     * Terminates when the thread is interrupted (on shutdown).
     */
    private void drainLoop(SubscriberHandle handle) {
        ExcerptTailer tailer = handle.tailer();
        Subscriber subscriber = handle.subscriber();

        while (!Thread.currentThread().isInterrupted()) {
            try {
                String json = tailer.readText();
                if (json == null) {
                    // No events available — yield briefly before retrying
                    Thread.onSpinWait();
                    continue;
                }

                FailureEvent event = objectMapper.readValue(json, FailureEvent.class);
                handle.consumed().incrementAndGet();

                try {
                    subscriber.onEvent(event);
                } catch (Exception e) {
                    log.error("Subscriber [{}] threw on onEvent: {}", subscriber.name(), e.getMessage(), e);
                }

            } catch (IOException e) {
                log.error("EventBus drain error for subscriber [{}]: {}", subscriber.name(), e.getMessage(), e);
            } catch (Exception e) {
                if (Thread.currentThread().isInterrupted()) break;
                log.error("Unexpected error in drain loop for [{}]: {}", subscriber.name(), e.getMessage(), e);
            }
        }
        log.info("Drain loop terminated for subscriber [{}]", subscriber.name());
    }

    @PreDestroy
    public void close() {
        handles.forEach(h -> {
            try {
                h.tailer().close();
            } catch (Exception e) {
                log.warn("Error closing tailer for [{}]: {}", h.subscriber().name(), e.getMessage());
            }
        });
        appender.close();
        queue.close();
        log.info("EventBus shut down");
    }

    // =====================================================================
    // SubscriberHandle — holds the subscriber, its tailer, and consumed counter
    // =====================================================================

    public record SubscriberHandle(
        Subscriber subscriber,
        ExcerptTailer tailer,
        AtomicLong consumed
    ) {}
}
