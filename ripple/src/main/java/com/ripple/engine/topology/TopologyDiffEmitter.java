package com.ripple.engine.topology;

import com.ripple.model.GraphSnapshot;
import com.ripple.model.ServiceId;
import com.ripple.model.TopologyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Detects structural changes between two consecutive {@link GraphSnapshot}s and
 * emits typed {@link TopologyEvent} records to the event stream.
 *
 * <p>Called after every mutation to the {@link TopologyGraph} to produce a diff
 * between the previous and current snapshots.
 */
@Component
public final class TopologyDiffEmitter {

    private static final Logger log = LoggerFactory.getLogger(TopologyDiffEmitter.class);

    /**
     * Computes the diff between {@code previous} and {@code current} and invokes
     * {@code eventConsumer} for each structural change detected.
     *
     * @param previous       the snapshot before the mutation
     * @param current        the snapshot after the mutation
     * @param eventConsumer  called once per detected change
     */
    public void emitDiff(GraphSnapshot previous, GraphSnapshot current,
                         Consumer<TopologyEvent> eventConsumer) {
        List<TopologyEvent> events = new ArrayList<>();
        long version = current.version();

        // Nodes added
        for (ServiceId id : current.nodes().keySet()) {
            if (!previous.hasNode(id)) {
                events.add(TopologyEvent.nodeAdded(id, version));
                log.info("Topology: node added [{}]", id);
            }
        }

        // Nodes removed
        for (ServiceId id : previous.nodes().keySet()) {
            if (!current.hasNode(id)) {
                events.add(TopologyEvent.nodeRemoved(id, version));
                log.info("Topology: node removed [{}]", id);
            }
        }

        // Edges added/removed — compare adjacency maps
        for (ServiceId from : current.adjacency().keySet()) {
            Set<ServiceId> currentDeps = current.dependenciesOf(from);
            Set<ServiceId> previousDeps = previous.dependenciesOf(from);

            for (ServiceId to : currentDeps) {
                if (!previousDeps.contains(to)) {
                    events.add(TopologyEvent.edgeAdded(from, to, version));
                    log.info("Topology: edge added [{} → {}]", from, to);
                }
            }
            for (ServiceId to : previousDeps) {
                if (!currentDeps.contains(to)) {
                    events.add(TopologyEvent.edgeRemoved(from, to, version));
                    log.info("Topology: edge removed [{} → {}]", from, to);
                }
            }
        }

        events.forEach(eventConsumer);
    }
}
