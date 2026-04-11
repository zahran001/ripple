package com.ripple.engine.probe;

import com.ripple.model.ProbeResult;
import com.ripple.model.ServiceNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;

/**
 * HTTP health-check prober using the JDK {@link HttpClient}.
 *
 * <p>Makes a GET request to the service's configured endpoint. A 2xx response is
 * treated as success. Any non-2xx response or connection failure is a FAILURE.
 *
 * <p>The per-request timeout is taken from the service node's probe interval or the
 * global default. Thread-safe — the underlying {@code HttpClient} is shared and
 * designed for concurrent use.
 */
public final class HttpProber implements Prober {

    private static final Logger log = LoggerFactory.getLogger(HttpProber.class);

    private final HttpClient httpClient;
    private final long timeoutMs;

    public HttpProber(long timeoutMs) {
        this.timeoutMs = timeoutMs;
        this.httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(timeoutMs))
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    }

    @Override
    public ProbeResult probe(ServiceNode node) {
        URI endpoint = node.endpoint();
        Instant start = Instant.now();

        try {
            var request = HttpRequest.newBuilder()
                .uri(endpoint)
                .GET()
                .timeout(Duration.ofMillis(timeoutMs))
                .header("User-Agent", "Ripple-Probe/1.0")
                .build();

            var response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());

            Duration latency = Duration.between(start, Instant.now());
            int statusCode = response.statusCode();

            if (statusCode >= 200 && statusCode < 300) {
                log.debug("HTTP probe SUCCESS for {} — {}ms, status {}", node.id(), latency.toMillis(), statusCode);
                return ProbeResult.success(node.id(), latency);
            } else {
                log.warn("HTTP probe FAILURE for {} — status {}", node.id(), statusCode);
                return ProbeResult.failure(node.id(), latency, "HTTP " + statusCode);
            }

        } catch (java.net.http.HttpTimeoutException e) {
            Duration latency = Duration.between(start, Instant.now());
            log.warn("HTTP probe TIMEOUT for {} after {}ms", node.id(), latency.toMillis());
            return ProbeResult.failure(node.id(), latency, "timeout: " + e.getMessage());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return ProbeResult.failure(node.id(), Duration.ZERO, "interrupted");

        } catch (Exception e) {
            Duration latency = Duration.between(start, Instant.now());
            log.warn("HTTP probe ERROR for {}: {}", node.id(), e.getMessage());
            return ProbeResult.failure(node.id(), latency, e.getMessage());
        }
    }
}
