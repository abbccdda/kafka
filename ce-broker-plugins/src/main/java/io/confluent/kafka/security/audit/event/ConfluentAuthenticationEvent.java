/*
 * Copyright [2020 - 2020] Confluent Inc.
 */
package io.confluent.kafka.security.audit.event;

import io.confluent.security.authorizer.Scope;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.apache.kafka.server.audit.AuditEventType;
import org.apache.kafka.server.audit.AuthenticationEvent;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class ConfluentAuthenticationEvent implements AuthenticationEvent {

    private final AuthenticationEvent authenticationEvent;
    private final Scope scope;

    public ConfluentAuthenticationEvent(AuthenticationEvent authenticationEvent, Scope scope) {
        this.authenticationEvent = authenticationEvent;
        this.scope = scope;
    }

    @Override
    public Optional<KafkaPrincipal> principal() {
        return authenticationEvent.principal();
    }

    @Override
    public AuthenticationContext authenticationContext() {
        return authenticationEvent.authenticationContext();
    }

    @Override
    public UUID uuid() {
        return authenticationEvent.uuid();
    }

    @Override
    public Instant timestamp() {
        return authenticationEvent.timestamp();
    }

    @Override
    public AuditEventType type() {
        return authenticationEvent.type();
    }

    @Override
    public AuditEventStatus status() {
        return authenticationEvent.status();
    }

    @Override
    public Map<String, Object> data() {
        return authenticationEvent.data();
    }

    public Scope getScope() {
        return scope;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ConfluentAuthenticationEvent that = (ConfluentAuthenticationEvent) o;
        return Objects.equals(authenticationEvent, that.authenticationEvent) &&
            Objects.equals(getScope(), that.getScope());
    }

    @Override
    public int hashCode() {
        return Objects.hash(authenticationEvent, getScope());
    }

    @Override
    public String toString() {
        return "ConfluentAuthenticationEvent{" +
            "authenticationEvent=" + authenticationEvent +
            ", scope=" + scope +
            '}';
    }
}
