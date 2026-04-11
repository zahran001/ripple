package com.ripple.model;

/**
 * Normalized severity score for a service in a blast radius computation.
 *
 * <p>Raw score formula: {@code criticality / (depth + 1)}<br>
 * Normalized to [0,1] across the full affected set.<br>
 * Services at {@code ProbeStatus.DEGRADED} receive a {@code 0.5} multiplier on their raw
 * score before normalization — they are impaired but reachable, not hard-down.
 *
 * <p>Scores are relative severity rankings, not probability estimates.
 * The weights (criticality scale, multiplier) are configurable starting-point defaults.
 *
 * @param raw       the un-normalized score (criticality / (depth + 1), with DEGRADED multiplier applied)
 * @param normalized the final [0,1] score after normalizing across the affected set
 * @param depth     BFS depth from the failed root (1 = direct dependent)
 */
public record DegradationScore(double raw, double normalized, int depth) {

    public static DegradationScore of(int criticality, int depth, boolean isDegraded) {
        double raw = (double) criticality / (depth + 1);
        if (isDegraded) {
            raw = raw * 0.5;
        }
        // normalized is set after the full affected set is computed
        return new DegradationScore(raw, 0.0, depth);
    }

    public DegradationScore withNormalized(double normalized) {
        return new DegradationScore(raw, normalized, depth);
    }
}
