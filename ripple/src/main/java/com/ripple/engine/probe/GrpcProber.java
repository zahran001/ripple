package com.ripple.engine.probe;

import com.ripple.model.ProbeResult;
import com.ripple.model.ServiceNode;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * gRPC health-check prober implementing the standard {@code grpc.health.v1.Health/Check} protocol.
 *
 * <p>Creates a new channel per probe to avoid connection state issues. Channels are
 * shut down immediately after the probe completes.
 *
 * <p>Thread-safe — no shared mutable state.
 */
public final class GrpcProber implements Prober {

    private static final Logger log = LoggerFactory.getLogger(GrpcProber.class);

    private final long timeoutMs;

    public GrpcProber(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    public ProbeResult probe(ServiceNode node) {
        var uri = node.endpoint();
        int port = uri.getPort() > 0 ? uri.getPort() : 50051;
        Instant start = Instant.now();
        ManagedChannel channel = null;

        try {
            channel = ManagedChannelBuilder
                .forAddress(uri.getHost(), port)
                .usePlaintext()
                .build();

            var stub = HealthGrpc.newBlockingStub(channel)
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);

            var request = HealthCheckRequest.newBuilder()
                .setService("")  // empty = overall server health
                .build();

            HealthCheckResponse response = stub.check(request);
            Duration latency = Duration.between(start, Instant.now());

            if (response.getStatus() == HealthCheckResponse.ServingStatus.SERVING) {
                log.debug("gRPC probe SUCCESS for {} — {}ms", node.id(), latency.toMillis());
                return ProbeResult.success(node.id(), latency);
            } else {
                String detail = "gRPC health status: " + response.getStatus();
                log.warn("gRPC probe UNHEALTHY for {}: {}", node.id(), detail);
                return ProbeResult.failure(node.id(), latency, detail);
            }

        } catch (io.grpc.StatusRuntimeException e) {
            Duration latency = Duration.between(start, Instant.now());
            log.warn("gRPC probe FAILURE for {}: {} {}", node.id(), e.getStatus().getCode(), e.getStatus().getDescription());
            return ProbeResult.failure(node.id(), latency, e.getStatus().toString());

        } catch (Exception e) {
            Duration latency = Duration.between(start, Instant.now());
            log.warn("gRPC probe ERROR for {}: {}", node.id(), e.getMessage());
            return ProbeResult.failure(node.id(), latency, e.getMessage());

        } finally {
            if (channel != null) {
                channel.shutdownNow();
            }
        }
    }
}
