package com.ripple.model;

import java.time.Instant;

/**
 * A runbook suggestion emitted by the degradation planner when a failure event
 * matches a registered runbook pattern.
 *
 * <p>Suggestions are ordered by the blast radius severity score of the originating
 * service — higher-severity failures surface their runbooks first.
 */
public record RunbookSuggestion(
    ServiceId affectedService,
    String runbookName,
    String runbookUrl,
    double severityScore,
    Instant generatedAt
) {}
