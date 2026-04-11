package com.ripple.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Typed wrapper around a service identifier string.
 *
 * <p>Prevents raw {@code String} values from being passed where a {@code ServiceId}
 * is expected — makes the compiler enforce service identity boundaries.
 *
 * <p>Immutable and safe to use as a map key.
 */
public record ServiceId(@JsonValue String value) {

    public ServiceId {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("ServiceId value must not be blank");
        }
    }

    @JsonCreator
    public static ServiceId of(String value) {
        return new ServiceId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
