package com.ripple.engine.stream;

import com.ripple.engine.blast.BlastRadiusEngine;
import com.ripple.engine.stream.alert.AlertRouterSubscriber;
import com.ripple.engine.topology.TopologyGraph;
import com.ripple.model.BlastRadiusResult;
import com.ripple.model.EventType;
import com.ripple.model.FailureEvent;
import com.ripple.model.ProbeResult;
import com.ripple.model.ProbeStatus;
import com.ripple.model.ServiceId;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wires all engines together: receives probe results, computes blast radius,
 * creates failure events, and publishes to the event bus.
 *
 * <p><strong>Data flow:</strong>
 * <pre>
 * ProbeScheduler
 *   → ProbeResult
 *     → RippleOrchestrator.handleProbeResult()
 *       → liveProbeStatuses.put(serviceId, status)
 *       → if FAILURE/DEGRADED: BlastRadiusEngine.compute()
 *         → FailureEvent.failure(type, origin, blastRadius)
 *           → EventBus.publish(event)  [stamps eventIndex]
 *             → fan-out to all Subscribers
 * </pre>
 *
 * <p>Thread-safe — called concurrently from multiple virtual probe threads.
 */
@Service
public class RippleOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(RippleOrchestrator.class);

    private final EventBus eventBus;
    private final TopologyGraph topologyGraph;
    private final BlastRadiusEngine blastRadiusEngine;
    private final ConcurrentHashMap<ServiceId, ProbeStatus> liveProbeStatuses;
    private final MeterRegistry meterRegistry;

    private final AlertRouterSubscriber alertRouterSubscriber;
    private final StateStoreSubscriber stateStoreSubscriber;
    private final SseSubscriber sseSubscriber;
    private final DegradationPlannerSubscriber degradationPlannerSubscriber;

    public RippleOrchestrator(EventBus eventBus,
                              TopologyGraph topologyGraph,
                              BlastRadiusEngine blastRadiusEngine,
                              ConcurrentHashMap<ServiceId, ProbeStatus> liveProbeStatuses,
                              MeterRegistry meterRegistry,
                              AlertRouterSubscriber alertRouterSubscriber,
                              StateStoreSubscriber stateStoreSubscriber,
                              SseSubscriber sseSubscriber,
                              DegradationPlannerSubscriber degradationPlannerSubscriber) {
        this.eventBus = eventBus;
        this.topologyGraph = topologyGraph;
        this.blastRadiusEngine = blastRadiusEngine;
        this.liveProbeStatuses = liveProbeStatuses;
        this.meterRegistry = meterRegistry;
        this.alertRouterSubscriber = alertRouterSubscriber;
        this.stateStoreSubscriber = stateStoreSubscriber;
        this.sseSubscriber = sseSubscriber;
        this.degradationPlannerSubscriber = degradationPlannerSubscriber;
    }

    @PostConstruct
    public void registerSubscribers() {
        eventBus.subscribe(alertRouterSubscriber);
        eventBus.subscribe(stateStoreSubscriber);
        eventBus.subscribe(sseSubscriber);
        eventBus.subscribe(degradationPlannerSubscriber);
        log.info("All subscribers registered with EventBus");
    }

    /**
     * Handles a probe result. Updates live status map, computes blast radius on failures,
     * and publishes the appropriate failure event to the bus.
     */
    public void handleProbeResult(ProbeResult result) {
        ServiceId serviceId = result.serviceId();
        ProbeStatus newStatus = result.status();
        ProbeStatus previousStatus = liveProbeStatuses.put(serviceId, newStatus);

        recordProbeLatency(serviceId, result.latency());

        if (newStatus == ProbeStatus.CIRCUIT_OPEN) {
            return;
        }

        EventType eventType = determineEventType(previousStatus, newStatus);
        if (eventType == null) return;

        log.debug("Probe result: [{}] {} → {} → emitting {}", serviceId, previousStatus, newStatus, eventType);

        FailureEvent event;
        if (newStatus == ProbeStatus.FAILURE || newStatus == ProbeStatus.DEGRADED) {
            BlastRadiusResult blastRadius = blastRadiusEngine.compute(
                serviceId, topologyGraph.snapshot(), Map.copyOf(liveProbeStatuses)
            );
            event = FailureEvent.failure(eventType, serviceId, blastRadius);
        } else {
            event = FailureEvent.topology(eventType, serviceId);
        }

        eventBus.publish(event);
    }

    private EventType determineEventType(ProbeStatus previous, ProbeStatus current) {
        if (previous == current) return null;

        return switch (current) {
            case FAILURE  -> EventType.SERVICE_FAILURE;
            case DEGRADED -> EventType.SERVICE_DEGRADED;
            case SUCCESS  -> (previous == ProbeStatus.FAILURE || previous == ProbeStatus.DEGRADED)
                ? EventType.SERVICE_RECOVERED : null;
            default -> null;
        };
    }

    private void recordProbeLatency(ServiceId serviceId, Duration latency) {
        Timer.builder("probe.latency")
            .tag("service", serviceId.value())
            .publishPercentiles(0.99, 0.95, 0.50)
            .register(meterRegistry)
            .record(latency);
    }
}
