package com.ripple.engine.blast;

import com.ripple.model.AffectedSet;
import com.ripple.model.BlastRadiusResult;
import com.ripple.model.GraphSnapshot;
import com.ripple.model.Protocol;
import com.ripple.model.ProbeStatus;
import com.ripple.model.ServiceId;
import com.ripple.model.ServiceNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BlastRadiusEngine} and {@link FailureSimulator}.
 *
 * <p>Uses hand-crafted {@link GraphSnapshot}s to exercise known topologies:
 * <ul>
 *   <li>Linear chain: root → A → B → C</li>
 *   <li>Diamond: root → L, root → R, L → sink, R → sink</li>
 *   <li>Wide fan-out: root → 10 leaves</li>
 *   <li>DEGRADED scoring: nodes at DEGRADED get 0.5× score before normalisation</li>
 *   <li>Unknown service: returns empty result without throwing</li>
 * </ul>
 */
class BlastRadiusEngineTest {

    private BlastRadiusEngine engine;
    private FailureSimulator simulator;

    @BeforeEach
    void setUp() {
        engine = new BlastRadiusEngine();
        simulator = new FailureSimulator(engine);
    }

    private ServiceNode node(String id, int criticality) {
        return new ServiceNode(
            ServiceId.of(id), id,
            Protocol.HTTP,
            URI.create("http://" + id + ":8080"),
            criticality,
            Duration.ofSeconds(5),
            500L
        );
    }

    private GraphSnapshot buildSnapshot(Map<ServiceId, Set<ServiceId>> adjacency,
                                        Map<ServiceId, ServiceNode> nodes) {
        // Build reverse adjacency
        var reverse = new HashMap<ServiceId, Set<ServiceId>>();
        for (ServiceId node : nodes.keySet()) {
            reverse.put(node, new HashSet<>());
        }
        for (var entry : adjacency.entrySet()) {
            for (ServiceId dep : entry.getValue()) {
                reverse.computeIfAbsent(dep, k -> new HashSet<>()).add(entry.getKey());
            }
        }
        return new GraphSnapshot(adjacency, reverse, nodes, java.time.Instant.now(), 1L);
    }

    @Test
    void linear_chain_failure_propagates_upward() {
        // root is a DB; A, B, C all depend on root (directly or transitively)
        // adjacency: A→root, B→A, C→B
        var root = ServiceId.of("root");
        var a = ServiceId.of("A");
        var b = ServiceId.of("B");
        var c = ServiceId.of("C");

        var adjacency = new HashMap<ServiceId, Set<ServiceId>>();
        adjacency.put(root, new HashSet<>());
        adjacency.put(a, new HashSet<>(Set.of(root)));
        adjacency.put(b, new HashSet<>(Set.of(a)));
        adjacency.put(c, new HashSet<>(Set.of(b)));

        var nodes = new HashMap<ServiceId, ServiceNode>();
        nodes.put(root, node("root", 10));
        nodes.put(a, node("A", 8));
        nodes.put(b, node("B", 6));
        nodes.put(c, node("C", 4));

        var snapshot = buildSnapshot(adjacency, nodes);
        var statuses = new HashMap<ServiceId, ProbeStatus>();
        statuses.put(root, ProbeStatus.FAILURE);
        statuses.put(a, ProbeStatus.SUCCESS);
        statuses.put(b, ProbeStatus.SUCCESS);
        statuses.put(c, ProbeStatus.SUCCESS);

        BlastRadiusResult result = engine.compute(root, snapshot, statuses);

        assertThat(result.affected().services()).containsExactlyInAnyOrder(a, b, c);
        assertThat(result.isComplete()).isTrue();

        // A is at depth 1, B at depth 2, C at depth 3
        // A raw = 8/2 = 4, B raw = 6/3 = 2, C raw = 4/4 = 1
        // After normalisation: A=1.0, B=0.5, C=0.25
        assertThat(result.affected().scoreFor(a).normalized()).isEqualTo(1.0);
        assertThat(result.affected().scoreFor(b).normalized()).isEqualTo(0.5);
        assertThat(result.affected().scoreFor(c).normalized()).isCloseTo(0.25, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    void diamond_topology_includes_sink_once() {
        // root → L, root → R; L → sink, R → sink
        var root = ServiceId.of("root");
        var l = ServiceId.of("L");
        var r = ServiceId.of("R");
        var sink = ServiceId.of("sink");

        var adjacency = new HashMap<ServiceId, Set<ServiceId>>();
        adjacency.put(root, new HashSet<>());
        adjacency.put(l, new HashSet<>(Set.of(root)));
        adjacency.put(r, new HashSet<>(Set.of(root)));
        adjacency.put(sink, new HashSet<>(Set.of(l, r)));

        var nodes = new HashMap<ServiceId, ServiceNode>();
        nodes.put(root, node("root", 10));
        nodes.put(l, node("L", 8));
        nodes.put(r, node("R", 8));
        nodes.put(sink, node("sink", 5));

        var snapshot = buildSnapshot(adjacency, nodes);
        var statuses = Map.of(root, ProbeStatus.FAILURE, l, ProbeStatus.SUCCESS,
            r, ProbeStatus.SUCCESS, sink, ProbeStatus.SUCCESS);

        BlastRadiusResult result = engine.compute(root, snapshot, statuses);

        // sink appears in the affected set exactly once (diamond convergence)
        assertThat(result.affected().services()).contains(sink);
        assertThat(result.affected().services().stream().filter(s -> s.equals(sink)).count()).isEqualTo(1);
    }

    @Test
    void degraded_service_gets_half_score() {
        var root = ServiceId.of("root");
        var healthy = ServiceId.of("healthy");
        var degraded = ServiceId.of("degraded");

        var adjacency = new HashMap<ServiceId, Set<ServiceId>>();
        adjacency.put(root, new HashSet<>());
        adjacency.put(healthy, new HashSet<>(Set.of(root)));
        adjacency.put(degraded, new HashSet<>(Set.of(root)));

        var nodes = new HashMap<ServiceId, ServiceNode>();
        nodes.put(root, node("root", 10));
        nodes.put(healthy, node("healthy", 8)); // criticality 8
        nodes.put(degraded, node("degraded", 8)); // same criticality

        var snapshot = buildSnapshot(adjacency, nodes);
        var statuses = new HashMap<ServiceId, ProbeStatus>();
        statuses.put(root, ProbeStatus.FAILURE);
        statuses.put(healthy, ProbeStatus.SUCCESS);
        statuses.put(degraded, ProbeStatus.DEGRADED); // DEGRADED gets 0.5× multiplier

        BlastRadiusResult result = engine.compute(root, snapshot, statuses);

        double healthyRaw = result.affected().scoreFor(healthy).raw();
        double degradedRaw = result.affected().scoreFor(degraded).raw();

        // degraded raw = healthy raw × 0.5
        assertThat(degradedRaw).isCloseTo(healthyRaw * 0.5, org.assertj.core.data.Offset.offset(0.001));
    }

    @Test
    void unknown_service_returns_empty_result_without_throwing() {
        var snapshot = GraphSnapshot.empty();
        var result = engine.compute(ServiceId.of("ghost"), snapshot, Map.of());

        assertThat(result.affected().isEmpty()).isTrue();
        assertThat(result.isComplete()).isTrue();
    }

    @Test
    void failure_simulator_assumes_all_others_healthy() {
        var root = ServiceId.of("root");
        var child = ServiceId.of("child");

        var adjacency = new HashMap<ServiceId, Set<ServiceId>>();
        adjacency.put(root, new HashSet<>());
        adjacency.put(child, new HashSet<>(Set.of(root)));

        var nodes = new HashMap<ServiceId, ServiceNode>();
        nodes.put(root, node("root", 10));
        nodes.put(child, node("child", 5));

        var snapshot = buildSnapshot(adjacency, nodes);
        var result = simulator.simulate(root, snapshot);

        // child should be in blast radius
        assertThat(result.affected().services()).contains(child);
    }

    @Test
    void normalised_scores_are_bounded_0_to_1() {
        var root = ServiceId.of("root");
        var adjacency = new HashMap<ServiceId, Set<ServiceId>>();
        var nodes = new HashMap<ServiceId, ServiceNode>();

        adjacency.put(root, new HashSet<>());
        nodes.put(root, node("root", 10));

        for (int i = 0; i < 10; i++) {
            var id = ServiceId.of("svc-" + i);
            adjacency.put(id, new HashSet<>(Set.of(root)));
            nodes.put(id, node("svc-" + i, (i % 10) + 1));
        }

        var snapshot = buildSnapshot(adjacency, nodes);
        var statuses = new HashMap<ServiceId, ProbeStatus>();
        snapshot.nodes().keySet().forEach(id -> statuses.put(id, ProbeStatus.SUCCESS));
        statuses.put(root, ProbeStatus.FAILURE);

        BlastRadiusResult result = engine.compute(root, snapshot, statuses);
        result.affected().asMap().values().forEach(score ->
            assertThat(score.normalized()).isBetween(0.0, 1.0)
        );
    }
}
