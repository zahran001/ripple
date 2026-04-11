package com.ripple.model;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Ordered mapping of {@link ServiceId} to {@link DegradationScore} for all services
 * affected by a given failure.
 *
 * <p>Services are ordered by normalized score descending (highest severity first).
 * Normalization is applied across the full set — scores are relative rankings,
 * not absolute probabilities.
 */
public final class AffectedSet {

    /** Raw scores keyed by ServiceId, insertion-ordered after normalization. */
    private final Map<ServiceId, DegradationScore> scores;

    private AffectedSet(Map<ServiceId, DegradationScore> scores) {
        this.scores = Collections.unmodifiableMap(scores);
    }

    /**
     * Builds an {@code AffectedSet} from a map of raw scores, normalizing them
     * to [0,1] based on the maximum raw score in the set.
     */
    public static AffectedSet fromRaw(Map<ServiceId, DegradationScore> rawScores) {
        if (rawScores.isEmpty()) {
            return new AffectedSet(Map.of());
        }

        double maxRaw = rawScores.values().stream()
            .mapToDouble(DegradationScore::raw)
            .max()
            .orElse(1.0);

        if (maxRaw == 0.0) maxRaw = 1.0;  // guard against division by zero

        final double max = maxRaw;
        Map<ServiceId, DegradationScore> normalized = rawScores.entrySet().stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().withNormalized(e.getValue().raw() / max)))
            .sorted(Map.Entry.<ServiceId, DegradationScore>comparingByValue(
                Comparator.comparingDouble(DegradationScore::normalized).reversed()))
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (a, b) -> a,
                LinkedHashMap::new
            ));

        return new AffectedSet(normalized);
    }

    public boolean contains(ServiceId id) {
        return scores.containsKey(id);
    }

    public DegradationScore scoreFor(ServiceId id) {
        return scores.get(id);
    }

    public Set<ServiceId> services() {
        return scores.keySet();
    }

    public Map<ServiceId, DegradationScore> asMap() {
        return scores;
    }

    public boolean isEmpty() {
        return scores.isEmpty();
    }

    public int size() {
        return scores.size();
    }

    @Override
    public String toString() {
        return "AffectedSet" + scores;
    }
}
