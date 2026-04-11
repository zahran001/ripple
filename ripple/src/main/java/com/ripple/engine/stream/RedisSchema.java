package com.ripple.engine.stream;

/**
 * Centralised Redis key schema. Never inline Redis key strings in production code — always
 * use a static method from this class.
 *
 * <p>Key namespacing convention: {@code ripple:{entity}:{id}:{attribute}}
 */
public final class RedisSchema {

    private RedisSchema() {}

    /** Current health state for a service: {@code ripple:service:{id}:health} */
    public static String serviceHealthKey(String serviceId) {
        return "ripple:service:" + serviceId + ":health";
    }

    /** Circuit breaker state for a service: {@code ripple:service:{id}:circuit} */
    public static String circuitBreakerKey(String serviceId) {
        return "ripple:service:" + serviceId + ":circuit";
    }

    /** Steady-state baseline metrics: {@code ripple:steady-state:{name}:baseline} */
    public static String steadyStateBaselineKey(String name) {
        return "ripple:steady-state:" + name + ":baseline";
    }

    /** Token bucket for rate limiting: {@code ripple:rate-limit:{callerId}} */
    public static String rateLimitKey(String callerId) {
        return "ripple:rate-limit:" + callerId;
    }
}
