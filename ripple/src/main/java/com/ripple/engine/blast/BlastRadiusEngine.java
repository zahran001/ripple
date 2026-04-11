package com.ripple.engine.blast;

import com.ripple.model.AffectedSet;
import com.ripple.model.BlastRadiusResult;
import com.ripple.model.DegradationScore;
import com.ripple.model.FailureMode;
import com.ripple.model.GraphSnapshot;
import com.ripple.model.ProbeStatus;
import com.ripple.model.ServiceId;
import com.ripple.model.ServiceNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

/**
 * Computes the blast radius of a failed or hypothetically failed service.
 *
 * <p><strong>Algorithm:</strong> Parallel BFS over the reverse-adjacency map using
 * a {@link ForkJoinPool}. Each BFS level (direct dependents, then their dependents)
 * is processed in parallel. Depth is tracked per node for severity scoring.
 *
 * <p><strong>Severity scoring formula:</strong>
 * {@code rawScore = criticality / (depth + 1)}
 * where {@code criticality} ∈ [1,10] and {@code depth} is the BFS hop count from root.
 * Services at {@link ProbeStatus#DEGRADED} receive a 0.5 multiplier before normalization.
 * Scores are normalized to [0,1] across the full affected set.
 *
 * <p><strong>Partial results:</strong> When the snapshot contains edges pointing to
 * evicted nodes, the engine returns a partial result ({@code isComplete = false})
 * rather than throwing — graceful degradation (DDL-004).
 *
 * <p>Thread-safe — operates entirely on immutable {@link GraphSnapshot}s.
 */
@Service
public final class BlastRadiusEngine {

    private static final Logger log = LoggerFactory.getLogger(BlastRadiusEngine.class);

    private static final double DEGRADED_MULTIPLIER = 0.5;

    /**
     * Computes the blast radius for a given failed service using the supplied snapshot.
     *
     * @param failedService  the service that failed
     * @param snapshot       immutable topology snapshot
     * @param probeStatuses  current probe status per service (used for DEGRADED detection)
     * @return blast radius result, possibly partial if the graph is incomplete
     */
    public BlastRadiusResult compute(ServiceId failedService,
                                     GraphSnapshot snapshot,
                                     Map<ServiceId, ProbeStatus> probeStatuses) {
        Instant start = Instant.now();

        if (!snapshot.hasNode(failedService)) {
            log.warn("BlastRadiusEngine: unknown service [{}] in snapshot — returning empty result", failedService);
            return BlastRadiusResult.complete(failedService, AffectedSet.fromRaw(Map.of()),
                Duration.ZERO, snapshot.capturedAt());
        }

        Map<ServiceId, DegradationScore> rawScores = new ConcurrentHashMap<>();
        List<String> unresolvedEdges = new ArrayList<>();

        // Parallel BFS using ForkJoinPool
        ForkJoinPool.commonPool().invoke(new BfsTask(
            failedService, snapshot, probeStatuses, rawScores, unresolvedEdges, 1, new HashSet<>()
        ));

        AffectedSet affected = AffectedSet.fromRaw(rawScores);
        Duration computedIn = Duration.between(start, Instant.now());

        log.debug("Blast radius for [{}]: {} services affected in {}ms",
            failedService, affected.size(), computedIn.toMillis());

        if (!unresolvedEdges.isEmpty()) {
            return BlastRadiusResult.partial(failedService, affected, computedIn,
                snapshot.capturedAt(), unresolvedEdges);
        }
        return BlastRadiusResult.complete(failedService, affected, computedIn, snapshot.capturedAt());
    }

    // =====================================================================
    // BFS ForkJoinTask — processes one BFS level, forks children in parallel
    // =====================================================================

    private static final class BfsTask extends RecursiveTask<Void> {

        private final ServiceId node;
        private final GraphSnapshot snapshot;
        private final Map<ServiceId, ProbeStatus> probeStatuses;
        private final Map<ServiceId, DegradationScore> scores;
        private final List<String> unresolvedEdges;
        private final int depth;
        private final Set<ServiceId> visited;

        BfsTask(ServiceId node, GraphSnapshot snapshot,
                Map<ServiceId, ProbeStatus> probeStatuses,
                Map<ServiceId, DegradationScore> scores,
                List<String> unresolvedEdges,
                int depth,
                Set<ServiceId> visited) {
            this.node = node;
            this.snapshot = snapshot;
            this.probeStatuses = probeStatuses;
            this.scores = scores;
            this.unresolvedEdges = unresolvedEdges;
            this.depth = depth;
            this.visited = visited;
        }

        @Override
        protected Void compute() {
            Set<ServiceId> dependents = snapshot.dependentsOf(node);
            List<BfsTask> subTasks = new ArrayList<>();

            for (ServiceId dependent : dependents) {
                // Guard visited set — thread-safe because ConcurrentHashMap.putIfAbsent
                if (scores.putIfAbsent(dependent, computeScore(dependent, depth)) != null) {
                    continue; // already visited at a shallower depth
                }

                // Check for unresolved node (edge to evicted service)
                if (!snapshot.hasNode(dependent)) {
                    synchronized (unresolvedEdges) {
                        unresolvedEdges.add(node + " → " + dependent);
                    }
                    continue;
                }

                Set<ServiceId> nextVisited = new HashSet<>(visited);
                nextVisited.add(node);
                subTasks.add(new BfsTask(
                    dependent, snapshot, probeStatuses, scores,
                    unresolvedEdges, depth + 1, nextVisited
                ));
            }

            invokeAll(subTasks);
            return null;
        }

        private DegradationScore computeScore(ServiceId id, int depth) {
            ProbeStatus status = probeStatuses.getOrDefault(id, ProbeStatus.SUCCESS);
            boolean isDegraded = status == ProbeStatus.DEGRADED;
            ServiceNode node = snapshot.nodeFor(id);
            int criticality = node != null ? node.criticality() : 5; // default if node metadata missing
            return DegradationScore.of(criticality, depth, isDegraded);
        }
    }
}
