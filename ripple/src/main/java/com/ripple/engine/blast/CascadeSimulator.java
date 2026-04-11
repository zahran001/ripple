package com.ripple.engine.blast;

import com.ripple.model.AffectedSet;
import com.ripple.model.BlastRadiusResult;
import com.ripple.model.CascadeResult;
import com.ripple.model.FailureMode;
import com.ripple.model.GraphSnapshot;
import com.ripple.model.ProbeStatus;
import com.ripple.model.ServiceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

/**
 * Extends {@link FailureSimulator} to model cascading failure propagation.
 *
 * <p><strong>Algorithm — iterative wave propagation:</strong>
 * <pre>
 * wave 0:  mark root HARD_DOWN
 * wave 1:  for each node N whose deps contain any wave-0 node:
 *            if failedOrDegradedDeps(N) / totalDeps(N) >= cascadeThreshold → mark CASCADED
 * wave 2:  repeat for wave-1 nodes
 * ...
 * repeat until no new nodes added in a wave
 * </pre>
 *
 * <p>Each wave is processed in parallel using {@link ForkJoinPool#commonPool()}.
 * The DAG structure (no cycles by construction) guarantees termination.
 * In practice, real service graphs are shallow (depth 3–5) — this terminates quickly.
 *
 * <p>Thread-safe — operates entirely on a cloned probe-status map.
 */
@Service
public final class CascadeSimulator extends FailureSimulator {

    private static final Logger log = LoggerFactory.getLogger(CascadeSimulator.class);

    public CascadeSimulator(BlastRadiusEngine engine) {
        super(engine);
    }

    /**
     * Simulates cascading failure propagation from the given root node.
     *
     * @param root              the service to hypothetically fail
     * @param snapshot          current topology snapshot (not mutated)
     * @param cascadeThreshold  fraction of failed/degraded dependencies required to
     *                          trigger cascade (0.0–1.0, default 0.5)
     * @return cascade result including per-service failure modes and wave assignments
     */
    public CascadeResult simulate(ServiceId root, GraphSnapshot snapshot, double cascadeThreshold) {
        Map<ServiceId, FailureMode> failureModes = new ConcurrentHashMap<>();
        Map<ServiceId, Integer> waveAssignments = new ConcurrentHashMap<>();

        // Wave 0: mark the root as HARD_DOWN
        failureModes.put(root, FailureMode.HARD_DOWN);
        waveAssignments.put(root, 0);

        Set<ServiceId> currentWaveNodes = Set.of(root);
        int wave = 1;

        // Iterative wave propagation until stable
        while (true) {
            Set<ServiceId> nextWaveNodes = computeNextWave(
                currentWaveNodes, failureModes, snapshot, cascadeThreshold, wave, waveAssignments
            );

            if (nextWaveNodes.isEmpty()) break;  // cascade has stabilised

            failureModes.putAll(buildCascadeMap(nextWaveNodes));
            currentWaveNodes = nextWaveNodes;
            wave++;

            log.debug("Cascade wave {}: {} new nodes affected", wave - 1, nextWaveNodes.size());
        }

        int totalWaves = wave - 1;
        log.info("Cascade simulation for [{}] stabilised after {} waves — {} total affected",
            root, totalWaves, failureModes.size() - 1);

        // Now compute blast radius using the cascade status map
        Map<ServiceId, ProbeStatus> simulatedStatuses = buildCascadeProbeStatuses(failureModes, snapshot);
        BlastRadiusResult blastRadius = engine.compute(root, snapshot, simulatedStatuses);

        return new CascadeResult(blastRadius, failureModes, waveAssignments, cascadeThreshold, totalWaves);
    }

    private Set<ServiceId> computeNextWave(Set<ServiceId> prevWaveNodes,
                                            Map<ServiceId, FailureMode> failureModes,
                                            GraphSnapshot snapshot,
                                            double cascadeThreshold,
                                            int waveNumber,
                                            Map<ServiceId, Integer> waveAssignments) {
        Set<ServiceId> candidates = new HashSet<>();
        for (ServiceId affected : prevWaveNodes) {
            candidates.addAll(snapshot.dependentsOf(affected));
        }

        Set<ServiceId> nextWave = ConcurrentHashMap.newKeySet();
        List<Runnable> tasks = new ArrayList<>();

        for (ServiceId candidate : candidates) {
            if (failureModes.containsKey(candidate)) continue; // already cascaded or root

            tasks.add(() -> {
                Set<ServiceId> deps = snapshot.dependenciesOf(candidate);
                if (deps.isEmpty()) return;

                long failedOrDegraded = deps.stream()
                    .filter(dep -> failureModes.containsKey(dep) ||
                                   failureModes.getOrDefault(dep, null) == FailureMode.DEGRADED)
                    .count();

                double fraction = (double) failedOrDegraded / deps.size();
                if (fraction >= cascadeThreshold) {
                    nextWave.add(candidate);
                    waveAssignments.put(candidate, waveNumber);
                }
            });
        }

        // Execute all candidate evaluations in parallel
        if (!tasks.isEmpty()) {
            ForkJoinPool.commonPool().invokeAll(
                tasks.stream()
                    .map(t -> (java.util.concurrent.Callable<Void>) () -> { t.run(); return null; })
                    .toList()
            );
        }

        return nextWave;
    }

    private Map<ServiceId, FailureMode> buildCascadeMap(Set<ServiceId> nodes) {
        var map = new HashMap<ServiceId, FailureMode>();
        nodes.forEach(id -> map.put(id, FailureMode.CASCADED));
        return map;
    }

    private Map<ServiceId, ProbeStatus> buildCascadeProbeStatuses(Map<ServiceId, FailureMode> failureModes,
                                                                    GraphSnapshot snapshot) {
        var statuses = new HashMap<ServiceId, ProbeStatus>();
        snapshot.nodes().keySet().forEach(id -> statuses.put(id, ProbeStatus.SUCCESS));
        failureModes.forEach((id, mode) -> {
            switch (mode) {
                case HARD_DOWN, CASCADED -> statuses.put(id, ProbeStatus.FAILURE);
                case DEGRADED -> statuses.put(id, ProbeStatus.DEGRADED);
            }
        });
        return statuses;
    }
}
