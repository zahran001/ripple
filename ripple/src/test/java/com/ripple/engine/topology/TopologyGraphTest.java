package com.ripple.engine.topology;

import com.ripple.model.Protocol;
import com.ripple.model.ServiceId;
import com.ripple.model.ServiceNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link TopologyGraph}.
 *
 * <p>Tests: node add/remove, edge add/remove, cycle detection, snapshot immutability,
 * TTL stale node detection, and concurrent access correctness.
 */
class TopologyGraphTest {

    private TopologyGraph graph;

    private ServiceNode node(String id) {
        return ServiceNode.of(
            ServiceId.of(id), id,
            Protocol.HTTP,
            URI.create("http://" + id + ":8080"),
            Duration.ofSeconds(5),
            500L
        );
    }

    @BeforeEach
    void setUp() {
        graph = new TopologyGraph();
    }

    @Test
    void addNode_registers_service() {
        graph.addNode(node("frontend"));
        assertThat(graph.hasNode(ServiceId.of("frontend"))).isTrue();
        assertThat(graph.nodeCount()).isEqualTo(1);
    }

    @Test
    void addNode_is_idempotent() {
        graph.addNode(node("frontend"));
        graph.addNode(node("frontend")); // second call is a no-op
        assertThat(graph.nodeCount()).isEqualTo(1);
    }

    @Test
    void removeNode_removes_service_and_its_edges() {
        graph.addNode(node("frontend"));
        graph.addNode(node("backend"));
        graph.addEdge(ServiceId.of("frontend"), ServiceId.of("backend"));

        graph.removeNode(ServiceId.of("frontend"));

        assertThat(graph.hasNode(ServiceId.of("frontend"))).isFalse();
        // backend should still exist; no dangling reverse edge
        assertThat(graph.snapshot().dependentsOf(ServiceId.of("backend"))).doesNotContain(ServiceId.of("frontend"));
    }

    @Test
    void addEdge_builds_forward_and_reverse_adjacency() {
        graph.addNode(node("frontend"));
        graph.addNode(node("backend"));
        graph.addEdge(ServiceId.of("frontend"), ServiceId.of("backend"));

        var snapshot = graph.snapshot();
        assertThat(snapshot.dependenciesOf(ServiceId.of("frontend"))).contains(ServiceId.of("backend"));
        assertThat(snapshot.dependentsOf(ServiceId.of("backend"))).contains(ServiceId.of("frontend"));
    }

    @Test
    void addEdge_rejects_direct_cycle() {
        graph.addNode(node("A"));
        graph.addNode(node("B"));
        graph.addEdge(ServiceId.of("A"), ServiceId.of("B"));

        assertThatThrownBy(() -> graph.addEdge(ServiceId.of("B"), ServiceId.of("A")))
            .isInstanceOf(TopologyGraph.CycleDetectedException.class);
    }

    @Test
    void addEdge_rejects_transitive_cycle() {
        graph.addNode(node("A"));
        graph.addNode(node("B"));
        graph.addNode(node("C"));
        graph.addEdge(ServiceId.of("A"), ServiceId.of("B"));
        graph.addEdge(ServiceId.of("B"), ServiceId.of("C"));

        // A → B → C; adding C → A would create a cycle
        assertThatThrownBy(() -> graph.addEdge(ServiceId.of("C"), ServiceId.of("A")))
            .isInstanceOf(TopologyGraph.CycleDetectedException.class);
    }

    @Test
    void addEdge_does_not_reject_valid_dag() {
        // Diamond: A → B, A → C, B → D, C → D
        graph.addNode(node("A"));
        graph.addNode(node("B"));
        graph.addNode(node("C"));
        graph.addNode(node("D"));
        graph.addEdge(ServiceId.of("A"), ServiceId.of("B"));
        graph.addEdge(ServiceId.of("A"), ServiceId.of("C"));
        graph.addEdge(ServiceId.of("B"), ServiceId.of("D"));
        graph.addEdge(ServiceId.of("C"), ServiceId.of("D"));

        // No cycle should be detected
        assertThat(graph.nodeCount()).isEqualTo(4);
    }

    @Test
    void snapshot_is_immutable_copy() {
        graph.addNode(node("alpha"));
        var snapshot = graph.snapshot();

        graph.addNode(node("beta")); // mutate live graph

        // Snapshot should NOT reflect the mutation
        assertThat(snapshot.hasNode(ServiceId.of("beta"))).isFalse();
    }

    @Test
    void snapshot_version_increments_on_mutation() {
        long v0 = graph.snapshot().version();
        graph.addNode(node("svc-1"));
        long v1 = graph.snapshot().version();
        graph.addNode(node("svc-2"));
        long v2 = graph.snapshot().version();

        assertThat(v1).isGreaterThan(v0);
        assertThat(v2).isGreaterThan(v1);
    }

    @Test
    void stale_nodes_returns_ids_older_than_threshold() throws InterruptedException {
        graph.addNode(node("fresh"));
        graph.addNode(node("stale"));

        // Touch 'fresh' to update its timestamp
        Thread.sleep(60);
        graph.touch(ServiceId.of("fresh")); // update timestamp

        // stale threshold = 50ms, fresh was touched <50ms ago
        var staleNodes = graph.staleNodes(50);
        assertThat(staleNodes).contains(ServiceId.of("stale"));
        // 'fresh' was just touched — should not be stale
        assertThat(staleNodes).doesNotContain(ServiceId.of("fresh"));
    }

    @Test
    void concurrent_writes_are_safe() throws InterruptedException {
        int threads = 20;
        var latch = new CountDownLatch(threads);
        var errors = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            final int idx = i;
            Thread.ofVirtual().start(() -> {
                try {
                    graph.addNode(node("concurrent-" + idx));
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();

        assertThat(errors.get()).isZero();
        assertThat(graph.nodeCount()).isEqualTo(threads);
    }

    @Test
    void concurrent_reads_and_writes_are_safe() throws InterruptedException {
        graph.addNode(node("shared"));

        int writers = 5;
        int readers = 10;
        var latch = new CountDownLatch(writers + readers);
        var errors = new AtomicInteger(0);

        for (int i = 0; i < writers; i++) {
            final int idx = i;
            Thread.ofVirtual().start(() -> {
                try {
                    graph.addNode(node("w-" + idx));
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }
        for (int i = 0; i < readers; i++) {
            Thread.ofVirtual().start(() -> {
                try {
                    graph.snapshot(); // should never throw
                    graph.hasNode(ServiceId.of("shared"));
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        assertThat(errors.get()).isZero();
    }
}
