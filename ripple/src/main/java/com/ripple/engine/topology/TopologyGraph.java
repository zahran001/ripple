package com.ripple.engine.topology;

import com.ripple.model.GraphSnapshot;
import com.ripple.model.ServiceId;
import com.ripple.model.ServiceNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

/**
 * Thread-safe directed acyclic graph of service dependencies.
 *
 * <p><strong>Concurrency model:</strong>
 * <ul>
 *   <li>Reads use {@link StampedLock#tryOptimisticRead()} with fallback to a read lock.
 *       Under low write contention (the common case — topology is stable), reads acquire
 *       no lock at all.</li>
 *   <li>Structural mutations (addNode, addEdge, removeNode) acquire the write lock.</li>
 *   <li>All public methods are safe to call concurrently from multiple threads.</li>
 * </ul>
 *
 * <p><strong>CRITICAL — StampedLock reentrancy constraint:</strong><br>
 * {@link StampedLock} is NOT reentrant. A thread that already holds the lock and
 * tries to acquire it again will deadlock. The enforced pattern:
 * <ul>
 *   <li><strong>Public methods</strong> acquire the lock exactly once at the boundary,
 *       delegate to private helpers, and release on exit. NEVER call another public method.</li>
 *   <li><strong>Private methods</strong> contain the actual logic and never acquire the lock.</li>
 * </ul>
 *
 * <p>{@code adjacency} maps each service to the set of services it depends on.<br>
 * {@code reverseAdjacency} maps each service to the set of services that depend on it.
 */
@Component
public final class TopologyGraph {

    private static final Logger log = LoggerFactory.getLogger(TopologyGraph.class);

    private final StampedLock stampedLock = new StampedLock();

    private final ConcurrentHashMap<ServiceId, Set<ServiceId>> adjacency = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ServiceId, Set<ServiceId>> reverseAdjacency = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ServiceId, ServiceNode> nodes = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<ServiceId, Instant> lastSeen = new ConcurrentHashMap<>();

    private final AtomicLong version = new AtomicLong(0L);

    // =====================================================================
    // Public API — each method acquires the lock exactly once at the boundary
    // =====================================================================

    public void addNode(ServiceNode node) {
        long stamp = stampedLock.writeLock();
        try {
            addNodeInternal(node);
            version.incrementAndGet();
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    /**
     * Adds a directed dependency edge: {@code from} depends on {@code to}.
     * Runs Kahn's cycle detection — rejects cycles with {@link CycleDetectedException}.
     */
    public void addEdge(ServiceId from, ServiceId to) {
        long stamp = stampedLock.writeLock();
        try {
            ensureNodeExistsInternal(from);
            ensureNodeExistsInternal(to);

            if (wouldCreateCycleInternal(from, to)) {
                throw new CycleDetectedException(from, to);
            }

            adjacency.get(from).add(to);
            reverseAdjacency.get(to).add(from);
            version.incrementAndGet();
            log.debug("Edge added: {} → {}", from, to);
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    public void removeNode(ServiceId id) {
        long stamp = stampedLock.writeLock();
        try {
            removeNodeInternal(id);
            version.incrementAndGet();
        } finally {
            stampedLock.unlockWrite(stamp);
        }
    }

    public boolean hasNode(ServiceId id) {
        long stamp = stampedLock.tryOptimisticRead();
        boolean result = nodes.containsKey(id);
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                result = nodes.containsKey(id);
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return result;
    }

    public GraphSnapshot snapshot() {
        long stamp = stampedLock.tryOptimisticRead();
        GraphSnapshot snap = buildSnapshotInternal();
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                snap = buildSnapshotInternal();
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return snap;
    }

    public void touch(ServiceId id) {
        lastSeen.put(id, Instant.now());
    }

    public List<ServiceId> staleNodes(long thresholdMs) {
        long stamp = stampedLock.tryOptimisticRead();
        var result = findStaleNodesInternal(thresholdMs);
        if (!stampedLock.validate(stamp)) {
            stamp = stampedLock.readLock();
            try {
                result = findStaleNodesInternal(thresholdMs);
            } finally {
                stampedLock.unlockRead(stamp);
            }
        }
        return result;
    }

    public long version() {
        return version.get();
    }

    public int nodeCount() {
        return nodes.size();
    }

    // =====================================================================
    // Private helpers — never acquire the lock
    // =====================================================================

    private void addNodeInternal(ServiceNode node) {
        nodes.putIfAbsent(node.id(), node);
        adjacency.putIfAbsent(node.id(), new HashSet<>());
        reverseAdjacency.putIfAbsent(node.id(), new HashSet<>());
        lastSeen.put(node.id(), Instant.now());
    }

    /**
     * Ensures adjacency slots exist for a service ID referenced in an edge, even if
     * the node has not been explicitly registered via {@link #addNode}.
     *
     * <p><strong>Note:</strong> does NOT insert into {@code nodes} because
     * {@link ConcurrentHashMap} forbids null values and we have no {@link ServiceNode}
     * object here. Cycle detection therefore uses {@code adjacency.keySet()},
     * which covers all known endpoints whether or not they have a registered node.
     */
    private void ensureNodeExistsInternal(ServiceId id) {
        adjacency.putIfAbsent(id, new HashSet<>());
        reverseAdjacency.putIfAbsent(id, new HashSet<>());
        lastSeen.put(id, Instant.now());
    }

    private void removeNodeInternal(ServiceId id) {
        nodes.remove(id);
        lastSeen.remove(id);

        Set<ServiceId> dependencies = adjacency.remove(id);
        if (dependencies != null) {
            for (ServiceId dep : dependencies) {
                var reverseSet = reverseAdjacency.get(dep);
                if (reverseSet != null) reverseSet.remove(id);
            }
        }

        Set<ServiceId> dependents = reverseAdjacency.remove(id);
        if (dependents != null) {
            for (ServiceId dep : dependents) {
                var forwardSet = adjacency.get(dep);
                if (forwardSet != null) forwardSet.remove(id);
            }
        }
    }

    /**
     * Kahn's algorithm — returns {@code true} if adding {@code from → to} would create a cycle.
     */
    private boolean wouldCreateCycleInternal(ServiceId from, ServiceId to) {
        Map<ServiceId, Integer> inDegree = new HashMap<>();
        // Use adjacency.keySet() — covers all endpoints, registered or not
        for (ServiceId node : adjacency.keySet()) {
            inDegree.putIfAbsent(node, 0);
        }
        inDegree.putIfAbsent(from, 0);
        inDegree.putIfAbsent(to, 0);

        for (Map.Entry<ServiceId, Set<ServiceId>> entry : adjacency.entrySet()) {
            for (ServiceId dep : entry.getValue()) {
                inDegree.merge(dep, 1, Integer::sum);
            }
        }
        inDegree.merge(to, 1, Integer::sum);

        Queue<ServiceId> queue = new ArrayDeque<>();
        inDegree.forEach((node, degree) -> {
            if (degree == 0) queue.add(node);
        });

        int processed = 0;
        while (!queue.isEmpty()) {
            ServiceId current = queue.poll();
            processed++;

            Set<ServiceId> deps = adjacency.getOrDefault(current, Set.of());
            for (ServiceId dep : deps) {
                int newDegree = inDegree.merge(dep, -1, Integer::sum);
                if (newDegree == 0) queue.add(dep);
            }
            if (current.equals(from)) {
                int newDegree = inDegree.merge(to, -1, Integer::sum);
                if (newDegree == 0) queue.add(to);
            }
        }

        return processed < inDegree.size();
    }

    private GraphSnapshot buildSnapshotInternal() {
        return new GraphSnapshot(
            new HashMap<>(adjacency),
            new HashMap<>(reverseAdjacency),
            new HashMap<>(nodes),
            Instant.now(),
            version.get()
        );
    }

    private List<ServiceId> findStaleNodesInternal(long thresholdMs) {
        Instant cutoff = Instant.now().minusMillis(thresholdMs);
        var stale = new ArrayList<ServiceId>();
        lastSeen.forEach((id, seen) -> {
            if (seen.isBefore(cutoff)) stale.add(id);
        });
        return Collections.unmodifiableList(stale);
    }

    // =====================================================================
    // Exceptions
    // =====================================================================

    public static final class CycleDetectedException extends RuntimeException {
        private final ServiceId from;
        private final ServiceId to;

        public CycleDetectedException(ServiceId from, ServiceId to) {
            super("Adding edge " + from + " → " + to + " would create a cycle");
            this.from = from;
            this.to = to;
        }

        public ServiceId from() { return from; }
        public ServiceId to()   { return to; }
    }
}
