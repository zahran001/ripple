package com.ripple.engine.steadystate;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ripple.engine.stream.RedisSchema;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Persists a named metric baseline snapshot to Redis.
 *
 * <p>Used by {@code POST /steady-state/{name}/baseline} to capture current metric
 * values before chaos injection. After recovery, the evaluator can compare against
 * this baseline to verify the system returned to steady state.
 *
 * <p>Thread-safe — Redis operations are atomic.
 */
@Component
public final class BaselineStore {

    private static final Logger log = LoggerFactory.getLogger(BaselineStore.class);
    private static final Duration BASELINE_TTL = Duration.ofHours(24);

    private final StringRedisTemplate redisTemplate;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;

    public BaselineStore(StringRedisTemplate redisTemplate,
                         MeterRegistry meterRegistry,
                         ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.meterRegistry = meterRegistry;
        this.objectMapper = objectMapper;
    }

    /**
     * Snapshots current metric values and stores them under the given hypothesis name.
     *
     * @param name  the steady-state hypothesis name
     * @return the snapshot that was stored
     */
    public Map<String, Double> captureBaseline(String name) {
        Map<String, Double> snapshot = new HashMap<>();

        meterRegistry.getMeters().forEach(meter -> {
            String metricName = meter.getId().getName();
            try {
                double value = extractValue(meter);
                snapshot.put(metricName, value);
            } catch (Exception e) {
                log.debug("Could not extract value for metric {}: {}", metricName, e.getMessage());
            }
        });

        try {
            String json = objectMapper.writeValueAsString(snapshot);
            String key = RedisSchema.steadyStateBaselineKey(name);
            redisTemplate.opsForValue().set(key, json, BASELINE_TTL);
            log.info("Baseline captured for hypothesis [{}] — {} metrics", name, snapshot.size());
        } catch (Exception e) {
            log.error("Failed to persist baseline for [{}]: {}", name, e.getMessage(), e);
        }

        return snapshot;
    }

    /**
     * Retrieves the stored baseline for the given hypothesis name.
     *
     * @return the baseline snapshot, or an empty map if none exists
     */
    public Map<String, Double> getBaseline(String name) {
        try {
            String key = RedisSchema.steadyStateBaselineKey(name);
            String json = redisTemplate.opsForValue().get(key);
            if (json == null) return Map.of();
            return objectMapper.readValue(json, new TypeReference<Map<String, Double>>() {});
        } catch (Exception e) {
            log.error("Failed to retrieve baseline for [{}]: {}", name, e.getMessage(), e);
            return Map.of();
        }
    }

    private double extractValue(Meter meter) {
        return switch (meter) {
            case io.micrometer.core.instrument.Gauge g -> g.value();
            case io.micrometer.core.instrument.Counter c -> c.count();
            case io.micrometer.core.instrument.Timer t -> t.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS);
            default -> 0.0;
        };
    }
}
