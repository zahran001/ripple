package com.ripple.engine.probe;

import com.ripple.model.ProbeResult;
import com.ripple.model.ServiceNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Script-based health-check prober.
 *
 * <p>Executes a configurable shell command and interprets the exit code:
 * <ul>
 *   <li>Exit 0 — {@code SUCCESS}</li>
 *   <li>Any other exit code — {@code FAILURE}</li>
 *   <li>Timeout — {@code FAILURE}</li>
 * </ul>
 *
 * <p>The script path is taken from the service endpoint URI's path component,
 * e.g. {@code script:///opt/checks/myservice.sh}.
 *
 * <p>Thread-safe — creates a new process per probe.
 */
public final class ScriptProber implements Prober {

    private static final Logger log = LoggerFactory.getLogger(ScriptProber.class);

    private final long timeoutMs;

    public ScriptProber(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    public ProbeResult probe(ServiceNode node) {
        String scriptPath = node.endpoint().getPath();
        Instant start = Instant.now();

        try {
            var process = new ProcessBuilder(scriptPath)
                .redirectErrorStream(true)
                .start();

            boolean finished = process.waitFor(timeoutMs, TimeUnit.MILLISECONDS);
            Duration latency = Duration.between(start, Instant.now());

            if (!finished) {
                process.destroyForcibly();
                log.warn("Script probe TIMEOUT for {} after {}ms", node.id(), latency.toMillis());
                return ProbeResult.failure(node.id(), latency, "script timeout");
            }

            int exitCode = process.exitValue();
            if (exitCode == 0) {
                log.debug("Script probe SUCCESS for {} — {}ms", node.id(), latency.toMillis());
                return ProbeResult.success(node.id(), latency);
            } else {
                log.warn("Script probe FAILURE for {} — exit code {}", node.id(), exitCode);
                return ProbeResult.failure(node.id(), latency, "exit code " + exitCode);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ProbeResult.failure(node.id(), Duration.ZERO, "interrupted");

        } catch (Exception e) {
            Duration latency = Duration.between(start, Instant.now());
            log.warn("Script probe ERROR for {}: {}", node.id(), e.getMessage());
            return ProbeResult.failure(node.id(), latency, e.getMessage());
        }
    }
}
