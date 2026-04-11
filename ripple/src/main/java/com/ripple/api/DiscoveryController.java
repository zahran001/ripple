package com.ripple.api;

import com.ripple.engine.topology.TopologyDiffEmitter;
import com.ripple.engine.topology.TopologyGraph;
import com.ripple.model.GraphSnapshot;
import com.ripple.model.Protocol;
import com.ripple.model.ServiceId;
import com.ripple.model.ServiceNode;
import com.ripple.model.TopologyEvent;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * REST API for topology graph mutations: registering services and declaring dependencies.
 *
 * <p>Routes:
 * <ul>
 *   <li>{@code POST /topology/services}         — register a service node</li>
 *   <li>{@code DELETE /topology/services/{id}}   — remove a service node</li>
 *   <li>{@code POST /topology/edges}             — declare a dependency edge</li>
 *   <li>{@code GET /topology}                    — current topology snapshot</li>
 * </ul>
 */
@RestController
@RequestMapping("/topology")
public class DiscoveryController {

    private final TopologyGraph graph;
    private final TopologyDiffEmitter diffEmitter;
    private final Consumer<TopologyEvent> topologyEventPublisher;

    public DiscoveryController(TopologyGraph graph,
                               TopologyDiffEmitter diffEmitter,
                               Consumer<TopologyEvent> topologyEventPublisher) {
        this.graph = graph;
        this.diffEmitter = diffEmitter;
        this.topologyEventPublisher = topologyEventPublisher;
    }

    @PostMapping("/services")
    public ResponseEntity<ServiceRegistrationResponse> registerService(
            @Valid @RequestBody ServiceRegistrationRequest req) {

        GraphSnapshot before = graph.snapshot();

        ServiceNode node = new ServiceNode(
            ServiceId.of(req.id()),
            req.name(),
            req.protocol(),
            URI.create(req.endpoint()),
            req.criticality(),
            Duration.ofMillis(req.probeIntervalMs()),
            req.latencyThresholdMs()
        );

        graph.addNode(node);
        GraphSnapshot after = graph.snapshot();

        List<TopologyEvent> events = new ArrayList<>();
        diffEmitter.emitDiff(before, after, events::add);
        events.forEach(topologyEventPublisher);

        return ResponseEntity.ok(new ServiceRegistrationResponse(req.id(), after.version()));
    }

    @DeleteMapping("/services/{id}")
    public ResponseEntity<Void> removeService(@PathVariable String id) {
        GraphSnapshot before = graph.snapshot();
        graph.removeNode(ServiceId.of(id));
        GraphSnapshot after = graph.snapshot();

        List<TopologyEvent> events = new ArrayList<>();
        diffEmitter.emitDiff(before, after, events::add);
        events.forEach(topologyEventPublisher);

        return ResponseEntity.noContent().build();
    }

    @PostMapping("/edges")
    public ResponseEntity<EdgeResponse> addEdge(@Valid @RequestBody EdgeRequest req) {
        try {
            GraphSnapshot before = graph.snapshot();
            graph.addEdge(ServiceId.of(req.from()), ServiceId.of(req.to()));
            GraphSnapshot after = graph.snapshot();

            List<TopologyEvent> events = new ArrayList<>();
            diffEmitter.emitDiff(before, after, events::add);
            events.forEach(topologyEventPublisher);

            return ResponseEntity.ok(new EdgeResponse(req.from(), req.to(), after.version()));

        } catch (TopologyGraph.CycleDetectedException e) {
            return ResponseEntity.badRequest().build();
        }
    }

    @GetMapping
    public GraphSnapshot getTopology() {
        return graph.snapshot();
    }

    // =====================================================================
    // Request/response records
    // =====================================================================

    public record ServiceRegistrationRequest(
        @NotBlank String id,
        @NotBlank String name,
        Protocol protocol,
        @NotBlank String endpoint,
        @Min(1) @Max(10) int criticality,
        long probeIntervalMs,
        long latencyThresholdMs
    ) {}

    public record ServiceRegistrationResponse(String id, long topologyVersion) {}

    public record EdgeRequest(@NotBlank String from, @NotBlank String to) {}

    public record EdgeResponse(String from, String to, long topologyVersion) {}
}
