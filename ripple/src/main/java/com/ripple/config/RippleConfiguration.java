package com.ripple.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ripple.api.RateLimitInterceptor;
import com.ripple.engine.probe.ProbeScheduler;
import com.ripple.engine.stream.EventBus;
import com.ripple.engine.stream.RippleOrchestrator;
import com.ripple.engine.stream.alert.AlertRouterSubscriber;
import com.ripple.model.ProbeStatus;
import com.ripple.model.ServiceId;
import com.ripple.model.TopologyEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Spring bean wiring for the Ripple application.
 *
 * <p>Only beans that need explicit construction parameters are defined here.
 * Classes annotated with {@code @Component} or {@code @Service} are discovered
 * automatically by component scanning.
 */
@Configuration
public class RippleConfiguration {

    // =====================================================================
    // Jackson
    // =====================================================================

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    // =====================================================================
    // Event Bus — needs Chronicle Queue data directory from config
    // =====================================================================

    @Bean
    public EventBus eventBus(StreamProperties streamProperties, ObjectMapper objectMapper) {
        return new EventBus(streamProperties.dataDir(), objectMapper);
    }

    // =====================================================================
    // Live probe status map — shared mutable state, updated by RippleOrchestrator
    // =====================================================================

    @Bean
    public ConcurrentHashMap<ServiceId, ProbeStatus> liveProbeStatuses() {
        return new ConcurrentHashMap<>();
    }

    // =====================================================================
    // Probe Scheduler — needs orchestrator reference for probe result callback
    // =====================================================================

    @Bean
    public ProbeScheduler probeScheduler(ProbeProperties probeProps,
                                         CircuitBreakerProperties cbProps,
                                         RippleOrchestrator orchestrator) {
        return new ProbeScheduler(probeProps, cbProps, orchestrator::handleProbeResult);
    }

    // =====================================================================
    // Alert router — explicit construction; no @Component on AlertRouterSubscriber
    // =====================================================================

    @Bean
    public AlertRouterSubscriber alertRouterSubscriber() {
        return new AlertRouterSubscriber(List.of()); // configure sinks at runtime
    }

    // =====================================================================
    // Topology event publisher — bridges topology changes to the event bus
    // =====================================================================

    @Bean
    public Consumer<TopologyEvent> topologyEventPublisher(EventBus eventBus) {
        return topologyEvent -> {
            var failureEvent = com.ripple.model.FailureEvent.topology(
                topologyEvent.type(), topologyEvent.subject()
            );
            eventBus.publish(failureEvent);
        };
    }

    // =====================================================================
    // Spring MVC — rate limiter interceptor registration
    // =====================================================================

    @Bean
    public WebMvcConfigurer rateLimitWebMvcConfigurer(RateLimitInterceptor interceptor) {
        return new WebMvcConfigurer() {
            @Override
            public void addInterceptors(InterceptorRegistry registry) {
                registry.addInterceptor(interceptor)
                    .addPathPatterns("/topology/**", "/blast-radius/**",
                        "/steady-state/**", "/events/**");
            }
        };
    }
}
