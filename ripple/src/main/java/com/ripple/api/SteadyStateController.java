package com.ripple.api;

import com.ripple.engine.steadystate.BaselineStore;
import com.ripple.engine.steadystate.SteadyStateDefinition;
import com.ripple.engine.steadystate.SteadyStateEvaluator;
import com.ripple.engine.steadystate.SteadyStateResult;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST API for chaos engineering steady-state evaluation.
 *
 * <p>Routes:
 * <ul>
 *   <li>{@code POST /steady-state/{name}/baseline} — capture metric baseline before chaos</li>
 *   <li>{@code GET /steady-state/{name}/baseline}  — retrieve stored baseline</li>
 *   <li>{@code POST /steady-state/{name}/evaluate} — evaluate hypothesis against current metrics</li>
 * </ul>
 *
 * <p>Workflow:
 * <pre>
 * 1. POST /steady-state/my-hypothesis/baseline   → capture pre-chaos baseline
 * 2. Inject chaos (e.g., chaos.sh kill productcatalogservice)
 * 3. POST /steady-state/my-hypothesis/evaluate   → check if system is still steady
 * 4. GET /steady-state/my-hypothesis/baseline    → inspect captured values
 * </pre>
 */
@RestController
@RequestMapping("/steady-state")
public class SteadyStateController {

    private final SteadyStateEvaluator evaluator;
    private final BaselineStore baselineStore;

    public SteadyStateController(SteadyStateEvaluator evaluator, BaselineStore baselineStore) {
        this.evaluator = evaluator;
        this.baselineStore = baselineStore;
    }

    @PostMapping("/{name}/baseline")
    public ResponseEntity<Map<String, Double>> captureBaseline(@PathVariable String name) {
        Map<String, Double> snapshot = baselineStore.captureBaseline(name);
        return ResponseEntity.ok(snapshot);
    }

    @GetMapping("/{name}/baseline")
    public ResponseEntity<Map<String, Double>> getBaseline(@PathVariable String name) {
        Map<String, Double> baseline = baselineStore.getBaseline(name);
        if (baseline.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(baseline);
    }

    @PostMapping("/{name}/evaluate")
    public ResponseEntity<SteadyStateResult> evaluate(
            @PathVariable String name,
            @RequestBody(required = false) SteadyStateDefinitionRequest req) {

        SteadyStateDefinition definition = req != null
            ? new SteadyStateDefinition(
                req.probeLatencyP99MaxMs(),
                req.circuitBreakersAllClosed(),
                req.subscriberLagMax())
            : SteadyStateDefinition.defaults();

        SteadyStateResult result = evaluator.evaluate(name, definition);

        int httpStatus = result.status() == SteadyStateResult.SteadyStateStatus.STEADY ? 200 : 200;
        return ResponseEntity.status(httpStatus).body(result);
    }

    public record SteadyStateDefinitionRequest(
        long probeLatencyP99MaxMs,
        boolean circuitBreakersAllClosed,
        long subscriberLagMax
    ) {}
}
