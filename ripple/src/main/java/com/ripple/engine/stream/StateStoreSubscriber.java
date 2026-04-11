package com.ripple.engine.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ripple.model.FailureEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Persists failure events to durable storage.
 *
 * <p><strong>PostgreSQL:</strong> stores each {@link FailureEvent} as a JSON record
 * in the {@code failure_events} table. Source of truth for historical replay.
 *
 * <p><strong>Redis:</strong> writes current health state (latest probe result per service)
 * as fast-read keys. Used by the API layer to answer {@code GET /health} without
 * hitting PostgreSQL on every request.
 *
 * <p><strong>Backpressure tolerance:</strong> {@link Long#MAX_VALUE} — this subscriber
 * must never be shed. A missed event is a permanent gap in the historical record.
 * Replay and audit would be broken. This is unacceptable.
 *
 * <p>Thread-safe — called from a single drain virtual thread.
 */
@Component
public final class StateStoreSubscriber implements Subscriber {

    private static final Logger log = LoggerFactory.getLogger(StateStoreSubscriber.class);

    private final JdbcTemplate jdbcTemplate;
    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public StateStoreSubscriber(JdbcTemplate jdbcTemplate,
                                StringRedisTemplate redisTemplate,
                                ObjectMapper objectMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.redisTemplate = redisTemplate;
        this.objectMapper = objectMapper;
        ensureSchema();
    }

    @Override
    public void onEvent(FailureEvent event) {
        persistToPostgres(event);
        updateRedisState(event);
    }

    @Override
    public long highWaterMark() {
        return Long.MAX_VALUE; // NEVER shed — missed event = broken history
    }

    @Override
    public String name() {
        return "state-store";
    }

    private void persistToPostgres(FailureEvent event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            jdbcTemplate.update(
                "INSERT INTO failure_events (event_index, event_type, origin_service, payload, occurred_at) " +
                "VALUES (?, ?, ?, ?::jsonb, ?)",
                event.eventIndex(),
                event.type().name(),
                event.origin() != null ? event.origin().value() : null,
                json,
                java.sql.Timestamp.from(event.timestamp())
            );
            log.debug("StateStore persisted event [{}] index={}", event.type(), event.eventIndex());
        } catch (Exception e) {
            log.error("StateStore: failed to persist event [{}] to PostgreSQL: {}",
                event.type(), e.getMessage(), e);
        }
    }

    private void updateRedisState(FailureEvent event) {
        if (event.origin() == null) return;
        try {
            String key = RedisSchema.serviceHealthKey(event.origin().value());
            String value = event.type().name();
            redisTemplate.opsForValue().set(key, value, Duration.ofSeconds(300));
        } catch (Exception e) {
            log.warn("StateStore: failed to update Redis for [{}]: {}", event.origin(), e.getMessage());
        }
    }

    private void ensureSchema() {
        try {
            jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS failure_events (
                    id           BIGSERIAL PRIMARY KEY,
                    event_index  BIGINT,
                    event_type   VARCHAR(64) NOT NULL,
                    origin_service VARCHAR(255),
                    payload      JSONB NOT NULL,
                    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                )
                """);
            jdbcTemplate.execute(
                "CREATE INDEX IF NOT EXISTS idx_failure_events_occurred_at ON failure_events (occurred_at)");
            log.info("StateStore schema verified");
        } catch (Exception e) {
            log.warn("StateStore: schema creation skipped (may already exist): {}", e.getMessage());
        }
    }
}
