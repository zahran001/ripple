package com.ripple.engine.blast;

import com.ripple.model.BlastRadiusResult;
import com.ripple.model.GraphSnapshot;
import com.ripple.model.ProbeStatus;
import com.ripple.model.ServiceId;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Computes the blast radius for a hypothetically failed service without mutating the live graph.
 *
 * <p>Clones the topology snapshot, marks the target node as hypothetically failed by
 * injecting a {@link ProbeStatus#FAILURE} for it into the probe status map, then
 * delegates to {@link BlastRadiusEngine} for the BFS computation.
 *
 * <p>Thread-safe — operates entirely on immutable data structures and cloned state.
 */
@Service
public class FailureSimulator {

    protected final BlastRadiusEngine engine;

    public FailureSimulator(BlastRadiusEngine engine) {
        this.engine = engine;
    }

    /**
     * Simulates a failure for the given service and returns the blast radius.
     *
     * @param target   the service to hypothetically fail
     * @param snapshot the current topology snapshot (not mutated)
     * @return the computed blast radius
     */
    public BlastRadiusResult simulate(ServiceId target, GraphSnapshot snapshot) {
        // Build probe status map with the target marked as FAILURE
        Map<ServiceId, ProbeStatus> simulatedStatuses = buildSimulatedStatuses(target, snapshot);
        return engine.compute(target, snapshot, simulatedStatuses);
    }

    /**
     * Builds a probe status map where the target service is overridden to FAILURE.
     * All other services default to SUCCESS (healthy baseline assumption).
     */
    protected Map<ServiceId, ProbeStatus> buildSimulatedStatuses(ServiceId target,
                                                                   GraphSnapshot snapshot) {
        var statuses = new HashMap<ServiceId, ProbeStatus>();
        snapshot.nodes().keySet().forEach(id -> statuses.put(id, ProbeStatus.SUCCESS));
        statuses.put(target, ProbeStatus.FAILURE);
        return statuses;
    }
}
