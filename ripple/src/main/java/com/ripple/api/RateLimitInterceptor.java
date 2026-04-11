package com.ripple.api;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.concurrent.TimeUnit;

/**
 * Spring MVC interceptor that applies the token-bucket rate limit to all API requests.
 *
 * <p>The caller is identified by the first available of:
 * <ol>
 *   <li>{@code X-API-Key} header</li>
 *   <li>Forwarded IP ({@code X-Forwarded-For} header)</li>
 *   <li>Remote IP from the socket ({@code request.getRemoteAddr()})</li>
 * </ol>
 *
 * <p>Rejected requests receive:
 * <ul>
 *   <li>HTTP 429 Too Many Requests</li>
 *   <li>{@code Retry-After} header (seconds until next token)</li>
 *   <li>{@code X-RateLimit-Limit} and {@code X-RateLimit-Policy} headers</li>
 * </ul>
 */
@Component
public final class RateLimitInterceptor implements HandlerInterceptor {

    private static final Logger log = LoggerFactory.getLogger(RateLimitInterceptor.class);

    private final TokenBucketRateLimiter rateLimiter;

    public RateLimitInterceptor(TokenBucketRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    @Override
    public boolean preHandle(HttpServletRequest request,
                             HttpServletResponse response,
                             Object handler) throws Exception {

        // Skip rate limiting for Actuator/health endpoints
        String path = request.getRequestURI();
        if (path.startsWith("/actuator") || path.equals("/health")) {
            return true;
        }

        String callerId = resolveCallerId(request);

        if (rateLimiter.tryAcquire(callerId)) {
            return true; // token consumed; request proceeds
        }

        // Token bucket exhausted — reject with 429
        long retryAfterNanos = rateLimiter.nanosUntilNextToken(callerId);
        long retryAfterSeconds = Math.max(1, TimeUnit.NANOSECONDS.toSeconds(retryAfterNanos));

        response.setStatus(429);
        response.setHeader("Retry-After", String.valueOf(retryAfterSeconds));
        response.setHeader("Content-Type", "application/json");
        response.getWriter().write("""
            {"error":"rate_limit_exceeded","retryAfterSeconds":%d}
            """.formatted(retryAfterSeconds).trim());

        log.warn("Rate limit exceeded for caller [{}] on {}", callerId, path);
        return false;
    }

    private String resolveCallerId(HttpServletRequest request) {
        // 1. API key header (best — uniquely identifies the caller application)
        String apiKey = request.getHeader("X-API-Key");
        if (apiKey != null && !apiKey.isBlank()) return "key:" + apiKey;

        // 2. X-Forwarded-For (behind a reverse proxy)
        String forwarded = request.getHeader("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            return "ip:" + forwarded.split(",")[0].trim();
        }

        // 3. Direct socket IP (development / no proxy)
        return "ip:" + request.getRemoteAddr();
    }
}
