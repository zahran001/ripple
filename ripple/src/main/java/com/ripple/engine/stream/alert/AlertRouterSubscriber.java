package com.ripple.engine.stream.alert;

import com.ripple.engine.stream.Subscriber;
import com.ripple.model.FailureEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Routes failure events to configured alert sinks: PagerDuty, Slack webhook, or
 * generic HTTP webhook.
 *
 * <p><strong>Backpressure tolerance:</strong> {@link Long#MAX_VALUE} — this subscriber
 * must never be shed. A missed event is a missed incident alert: an on-call engineer
 * does not get paged. This is an unacceptable failure mode.
 *
 * <p>Sink failures are retried with exponential backoff starting at 1s, capped at 60s.
 * Events that exhaust all retries are written to the dead-letter log.
 *
 * <p>NOT a Spring @Component — instantiated as a @Bean in RippleConfiguration so that
 * the sinks list can be provided without ambiguous auto-wiring.
 */
public final class AlertRouterSubscriber implements Subscriber {

    private static final Logger log = LoggerFactory.getLogger(AlertRouterSubscriber.class);

    private static final int MAX_RETRIES = 5;
    private static final long INITIAL_DELAY_MS = 1000;
    private static final long MAX_DELAY_MS = 60_000;

    private final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(5))
        .build();

    private final List<AlertSink> sinks;
    private final AtomicLong alertsSent = new AtomicLong(0);
    private final AtomicLong alertsFailed = new AtomicLong(0);

    public AlertRouterSubscriber(List<AlertSink> sinks) {
        this.sinks = sinks;
        log.info("AlertRouterSubscriber started with {} configured sinks", sinks.size());
    }

    @Override
    public void onEvent(FailureEvent event) {
        if (!isAlertableEvent(event)) return;
        for (AlertSink sink : sinks) {
            routeWithRetry(event, sink);
        }
    }

    @Override
    public long highWaterMark() {
        return Long.MAX_VALUE;
    }

    @Override
    public String name() {
        return "alert-router";
    }

    public long alertsSent()   { return alertsSent.get(); }
    public long alertsFailed() { return alertsFailed.get(); }

    private boolean isAlertableEvent(FailureEvent event) {
        return switch (event.type()) {
            case SERVICE_FAILURE, CIRCUIT_OPENED -> true;
            default -> false;
        };
    }

    private void routeWithRetry(FailureEvent event, AlertSink sink) {
        long delay = INITIAL_DELAY_MS;
        Exception lastException = null;

        for (int attempt = 1; attempt <= MAX_RETRIES; attempt++) {
            try {
                sink.send(event, httpClient);
                alertsSent.incrementAndGet();
                return;
            } catch (Exception e) {
                lastException = e;
                log.warn("Alert to [{}] failed on attempt {}/{}: {}", sink.name(), attempt, MAX_RETRIES, e.getMessage());
                if (attempt < MAX_RETRIES) {
                    try { Thread.sleep(delay + (long)(Math.random() * 200)); }
                    catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
                    delay = Math.min(delay * 2, MAX_DELAY_MS);
                }
            }
        }

        alertsFailed.incrementAndGet();
        log.error("DEAD_LETTER: alert to [{}] exhausted {} retries for event [{}]: {}",
            sink.name(), MAX_RETRIES, event.type(),
            lastException != null ? lastException.getMessage() : "unknown");
    }

    public interface AlertSink {
        String name();
        void send(FailureEvent event, HttpClient httpClient) throws IOException, InterruptedException;
    }

    public record WebhookSink(String name, URI url) implements AlertSink {
        @Override
        public void send(FailureEvent event, HttpClient httpClient) throws IOException, InterruptedException {
            String body = """
                {"type":"%s","service":"%s","timestamp":"%s"}
                """.formatted(event.type(), event.origin(), event.timestamp()).trim();

            var request = HttpRequest.newBuilder()
                .uri(url)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .build();

            var response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() >= 400) {
                throw new IOException("Webhook returned HTTP " + response.statusCode());
            }
        }
    }
}
