package com.ripple.engine.stream;

import com.ripple.model.EventType;
import com.ripple.model.FailureEvent;
import com.ripple.model.ServiceId;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BackpressureMonitor}.
 *
 * <p>Core invariant: when a subscriber's lag exceeds its high-water mark, that subscriber
 * is shed (its tailer advances to tail). Other subscribers with higher tolerances are
 * completely unaffected.
 *
 * <p>Tests use stub {@link Subscriber} implementations to control lag values precisely.
 */
class BackpressureMonitorTest {

    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
    }

    /** Creates a stub subscriber handle with a fixed consumed counter. */
    private EventBus.SubscriberHandle stubHandle(String name, long highWaterMark, long consumed) {
        Subscriber subscriber = new Subscriber() {
            @Override public void onEvent(FailureEvent event) {}
            @Override public long highWaterMark() { return highWaterMark; }
            @Override public String name() { return name; }
        };
        AtomicLong consumedCount = new AtomicLong(consumed);
        var tailer = Mockito.mock(net.openhft.chronicle.queue.ExcerptTailer.class);
        return new EventBus.SubscriberHandle(subscriber, tailer, consumedCount);
    }

    @Test
    void subscriber_below_high_water_mark_is_not_shed() {
        EventBus mockBus = Mockito.mock(EventBus.class);
        Mockito.when(mockBus.producedCount()).thenReturn(50L);

        var handle = stubHandle("fast-sub", 100L, 45L); // lag = 5, hwm = 100
        Mockito.when(mockBus.handles()).thenReturn(List.of(handle));

        BackpressureMonitor monitor = new BackpressureMonitor(mockBus, meterRegistry);
        monitor.checkAndShed();

        // tailer.toEnd() should NOT have been called
        Mockito.verify(handle.tailer(), Mockito.never()).toEnd();
        assertThat(handle.consumed().get()).isEqualTo(45L); // unchanged
    }

    @Test
    void subscriber_above_high_water_mark_is_shed() {
        EventBus mockBus = Mockito.mock(EventBus.class);
        Mockito.when(mockBus.producedCount()).thenReturn(200L);

        var slowHandle = stubHandle("slow-sub", 100L, 0L); // lag = 200, hwm = 100
        Mockito.when(mockBus.handles()).thenReturn(List.of(slowHandle));

        BackpressureMonitor monitor = new BackpressureMonitor(mockBus, meterRegistry);
        monitor.checkAndShed();

        // tailer.toEnd() MUST have been called
        Mockito.verify(slowHandle.tailer(), Mockito.times(1)).toEnd();
        // consumed counter should be synced to produced
        assertThat(slowHandle.consumed().get()).isEqualTo(200L);
    }

    @Test
    void fast_subscriber_unaffected_when_slow_subscriber_is_shed() {
        EventBus mockBus = Mockito.mock(EventBus.class);
        Mockito.when(mockBus.producedCount()).thenReturn(200L);

        var slowHandle = stubHandle("slow-sub", 100L, 0L);   // lag=200, hwm=100 → shed
        var fastHandle = stubHandle("fast-sub", 1000L, 195L); // lag=5, hwm=1000 → keep

        Mockito.when(mockBus.handles()).thenReturn(List.of(slowHandle, fastHandle));

        BackpressureMonitor monitor = new BackpressureMonitor(mockBus, meterRegistry);
        monitor.checkAndShed();

        // slow is shed
        Mockito.verify(slowHandle.tailer(), Mockito.times(1)).toEnd();
        // fast is not shed
        Mockito.verify(fastHandle.tailer(), Mockito.never()).toEnd();
        assertThat(fastHandle.consumed().get()).isEqualTo(195L); // unchanged
    }

    @Test
    void never_shed_subscriber_with_max_value_hwm() {
        EventBus mockBus = Mockito.mock(EventBus.class);
        Mockito.when(mockBus.producedCount()).thenReturn(Long.MAX_VALUE / 2);

        // Alert router — hwm is Long.MAX_VALUE; consumed is 0 → massive lag, still not shed
        var criticalHandle = stubHandle("alert-router", Long.MAX_VALUE, 0L);
        Mockito.when(mockBus.handles()).thenReturn(List.of(criticalHandle));

        BackpressureMonitor monitor = new BackpressureMonitor(mockBus, meterRegistry);
        monitor.checkAndShed();

        Mockito.verify(criticalHandle.tailer(), Mockito.never()).toEnd();
    }

    @Test
    void lag_metric_is_recorded_for_all_subscribers() {
        EventBus mockBus = Mockito.mock(EventBus.class);
        Mockito.when(mockBus.producedCount()).thenReturn(100L);

        var handle = stubHandle("test-sub", 1000L, 80L);
        Mockito.when(mockBus.handles()).thenReturn(List.of(handle));

        BackpressureMonitor monitor = new BackpressureMonitor(mockBus, meterRegistry);
        monitor.checkAndShed();

        // lagFor() should return the lag based on produced - consumed
        Mockito.when(mockBus.handles()).thenReturn(List.of(handle));
        long lag = monitor.lagFor("test-sub");
        assertThat(lag).isEqualTo(20L); // 100 - 80
    }

    @Test
    void shed_increments_lagging_metric() {
        EventBus mockBus = Mockito.mock(EventBus.class);
        Mockito.when(mockBus.producedCount()).thenReturn(200L);

        var slowHandle = stubHandle("slow-sub", 50L, 0L); // lag=200, hwm=50
        Mockito.when(mockBus.handles()).thenReturn(List.of(slowHandle));

        BackpressureMonitor monitor = new BackpressureMonitor(mockBus, meterRegistry);
        monitor.checkAndShed();

        // subscriber.lagging counter should have been incremented
        double laggingCount = meterRegistry.counter("subscriber.lagging", "subscriber", "slow-sub").count();
        assertThat(laggingCount).isEqualTo(1.0);
    }
}
