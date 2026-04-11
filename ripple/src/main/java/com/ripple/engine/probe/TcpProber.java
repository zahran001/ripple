package com.ripple.engine.probe;

import com.ripple.model.ProbeResult;
import com.ripple.model.ServiceNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;

/**
 * Raw TCP connection health-check prober.
 *
 * <p>Attempts to establish a TCP connection to the service's host and port.
 * A successful TCP handshake is treated as a SUCCESS result. Connection refused or
 * timeout is a FAILURE.
 *
 * <p>Thread-safe — creates a new socket per probe attempt.
 */
public final class TcpProber implements Prober {

    private static final Logger log = LoggerFactory.getLogger(TcpProber.class);

    private final int timeoutMs;

    public TcpProber(long timeoutMs) {
        this.timeoutMs = (int) Math.min(timeoutMs, Integer.MAX_VALUE);
    }

    @Override
    public ProbeResult probe(ServiceNode node) {
        var uri = node.endpoint();
        int port = uri.getPort() > 0 ? uri.getPort() : 80;
        Instant start = Instant.now();

        try (var socket = new Socket()) {
            socket.connect(new InetSocketAddress(uri.getHost(), port), timeoutMs);
            Duration latency = Duration.between(start, Instant.now());
            log.debug("TCP probe SUCCESS for {} — {}ms", node.id(), latency.toMillis());
            return ProbeResult.success(node.id(), latency);

        } catch (java.net.SocketTimeoutException e) {
            Duration latency = Duration.between(start, Instant.now());
            log.warn("TCP probe TIMEOUT for {} after {}ms", node.id(), latency.toMillis());
            return ProbeResult.failure(node.id(), latency, "tcp timeout");

        } catch (Exception e) {
            Duration latency = Duration.between(start, Instant.now());
            log.warn("TCP probe ERROR for {}: {}", node.id(), e.getMessage());
            return ProbeResult.failure(node.id(), latency, e.getMessage());
        }
    }
}
