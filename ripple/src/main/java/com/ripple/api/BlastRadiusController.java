package com.ripple.api;

import com.ripple.engine.blast.BlastRadiusEngine;
import com.ripple.engine.blast.CascadeSimulator;
import com.ripple.engine.blast.FailureSimulator;
import com.ripple.engine.topology.TopologyGraph;
import com.ripple.model.BlastRadiusResult;
import com.ripple.model.CascadeResult;
import com.ripple.model.GraphSnapshot;
import com.ripple.model.ProbeStatus;
import com.ripple.model.ServiceId;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * REST API for blast radius computation and cascade simulation.
 *
 * <p>Routes:
 * <ul>
 *   <li>{@code GET /blast-radius/{serviceId}}           — compute blast radius for a failing service</li>
 *   <li>{@code POST /blast-radius/{serviceId}/simulate} — hypothetical failure simulation</li>
 *   <li>{@code POST /blast-radius/{serviceId}/cascade}  — cascade failure simulation</li>
 * </ul>
 */
@RestController
@RequestMapping("/blast-radius")
public class BlastRadiusController {

    private final TopologyGraph graph;
    private final BlastRadiusEngine blastRadiusEngine;
    private final FailureSimulator failureSimulator;
    private final CascadeSimulator cascadeSimulator;
    // Injected as ConcurrentHashMap to disambiguate from any other Map bean
    private final ConcurrentHashMap<ServiceId, ProbeStatus> liveProbeStatuses;

    public BlastRadiusController(TopologyGraph graph,
                                 BlastRadiusEngine blastRadiusEngine,
                                 FailureSimulator failureSimulator,
                                 CascadeSimulator cascadeSimulator,
                                 ConcurrentHashMap<ServiceId, ProbeStatus> liveProbeStatuses) {
        this.graph = graph;
        this.blastRadiusEngine = blastRadiusEngine;
        this.failureSimulator = failureSimulator;
        this.cascadeSimulator = cascadeSimulator;
        this.liveProbeStatuses = liveProbeStatuses;
    }

    @GetMapping("/{serviceId}")
    public ResponseEntity<BlastRadiusResult> getBlastRadius(@PathVariable String serviceId) {
        ServiceId id = ServiceId.of(serviceId);
        GraphSnapshot snapshot = graph.snapshot();

        if (!snapshot.hasNode(id)) {
            return ResponseEntity.notFound().build();
        }

        BlastRadiusResult result = blastRadiusEngine.compute(id, snapshot, Map.copyOf(liveProbeStatuses));
        return ResponseEntity.ok(result);
    }

    @PostMapping("/{serviceId}/simulate")
    public ResponseEntity<BlastRadiusResult> simulateFailure(@PathVariable String serviceId) {
        ServiceId id = ServiceId.of(serviceId);
        GraphSnapshot snapshot = graph.snapshot();

        if (!snapshot.hasNode(id)) {
            return ResponseEntity.notFound().build();
        }

        BlastRadiusResult result = failureSimulator.simulate(id, snapshot);
        return ResponseEntity.ok(result);
    }

    @PostMapping("/{serviceId}/cascade")
    public ResponseEntity<CascadeResult> simulateCascade(
            @PathVariable String serviceId,
            @RequestParam(defaultValue = "0.5") double cascadeThreshold) {

        ServiceId id = ServiceId.of(serviceId);
        GraphSnapshot snapshot = graph.snapshot();

        if (!snapshot.hasNode(id)) {
            return ResponseEntity.notFound().build();
        }

        if (cascadeThreshold < 0.0 || cascadeThreshold > 1.0) {
            return ResponseEntity.badRequest().build();
        }

        CascadeResult result = cascadeSimulator.simulate(id, snapshot, cascadeThreshold);
        return ResponseEntity.ok(result);
    }
}
