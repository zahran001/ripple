package com.ripple;

import com.ripple.config.CircuitBreakerProperties;
import com.ripple.config.ProbeProperties;
import com.ripple.config.RateLimitProperties;
import com.ripple.config.StreamProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Ripple — distributed failure blast-radius simulator.
 *
 * <p>Entry point. Spring MVC with {@code spring.threads.virtual.enabled=true} dispatches
 * all request handling on Java 21 virtual threads — no reactive operators required outside
 * the SSE endpoint.
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties({
    ProbeProperties.class,
    CircuitBreakerProperties.class,
    StreamProperties.class,
    RateLimitProperties.class
})
public class RippleApplication {

    public static void main(String[] args) {
        SpringApplication.run(RippleApplication.class, args);
    }
}
