package com.ripple.engine.stream;

import com.ripple.model.AffectedSet;
import com.ripple.model.FailureEvent;
import com.ripple.model.RunbookSuggestion;
import com.ripple.model.ServiceId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Matches failure events against a configured runbook registry and emits
 * {@link RunbookSuggestion} records ordered by blast radius severity score.
 *
 * <p><strong>Backpressure tolerance:</strong> Medium ({@code 1000}). Runbook suggestions
 * are best-effort — missing a suggestion is not a critical failure. Shedding is acceptable.
 *
 * <p>The runbook registry is loaded from {@code application.yml}. Suggestions are
 * pushed to registered consumers (typically the API layer's most-recent-suggestion cache).
 *
 * <p>Thread-safe — called from a single drain virtual thread; consumers are read concurrently.
 */
@Component
public final class DegradationPlannerSubscriber implements Subscriber {

    private static final Logger log = LoggerFactory.getLogger(DegradationPlannerSubscriber.class);

    private static final long DEGRADATION_PLANNER_HIGH_WATER_MARK = 1000L;

    // serviceId → {runbookName, runbookUrl}
    private final Map<String, RunbookEntry> runbookRegistry = new ConcurrentHashMap<>();

    // Most recent suggestions, keyed by service ID
    private final Map<ServiceId, RunbookSuggestion> latestSuggestions = new ConcurrentHashMap<>();

    private final List<Consumer<List<RunbookSuggestion>>> suggestionConsumers = new ArrayList<>();

    @Override
    public void onEvent(FailureEvent event) {
        if (event.blastRadius() == null) return;
        if (!isFailureEvent(event)) return;

        AffectedSet affected = event.blastRadius().affected();
        List<RunbookSuggestion> suggestions = new ArrayList<>();

        for (ServiceId serviceId : affected.services()) {
            RunbookEntry runbook = runbookRegistry.get(serviceId.value());
            if (runbook == null) continue;

            double score = affected.scoreFor(serviceId).normalized();
            var suggestion = new RunbookSuggestion(
                serviceId, runbook.name(), runbook.url(), score, Instant.now()
            );
            latestSuggestions.put(serviceId, suggestion);
            suggestions.add(suggestion);
            log.debug("Runbook suggestion: [{}] → {} (score={})", serviceId, runbook.name(), score);
        }

        if (!suggestions.isEmpty()) {
            // Order by severity score descending
            suggestions.sort((a, b) -> Double.compare(b.severityScore(), a.severityScore()));
            suggestionConsumers.forEach(c -> c.accept(suggestions));
        }
    }

    @Override
    public long highWaterMark() {
        return DEGRADATION_PLANNER_HIGH_WATER_MARK; // best-effort; shedding acceptable
    }

    @Override
    public String name() {
        return "degradation-planner";
    }

    /** Registers a runbook entry for the given service. Called at startup from config. */
    public void registerRunbook(String serviceId, String runbookName, String runbookUrl) {
        runbookRegistry.put(serviceId, new RunbookEntry(runbookName, runbookUrl));
    }

    /** Returns the most recent runbook suggestion for the given service, or null. */
    public RunbookSuggestion latestSuggestionFor(ServiceId serviceId) {
        return latestSuggestions.get(serviceId);
    }

    /** Registers a consumer to receive batches of suggestions when a failure event is processed. */
    public void addSuggestionConsumer(Consumer<List<RunbookSuggestion>> consumer) {
        suggestionConsumers.add(consumer);
    }

    private boolean isFailureEvent(FailureEvent event) {
        return switch (event.type()) {
            case SERVICE_FAILURE, SERVICE_DEGRADED, CIRCUIT_OPENED -> true;
            default -> false;
        };
    }

    private record RunbookEntry(String name, String url) {}
}
