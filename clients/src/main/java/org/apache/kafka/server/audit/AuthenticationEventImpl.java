// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.audit;

import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Lazy;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class AuthenticationEventImpl implements AuthenticationEvent {

    private final Instant timestamp;
    private final Lazy<UUID> lazyUUID;
    private final Optional<KafkaPrincipal> principal;
    private final AuthenticationContext authenticationContext;
    private final AuditEventStatus auditEventStatus;

    private Map<String, Object> data = new HashMap<>();

    public AuthenticationEventImpl(KafkaPrincipal principal,
                                   AuthenticationContext authenticationContext,
                                   AuditEventStatus auditEventStatus,
                                   Instant timestamp) {
        this.principal = Optional.of(principal);
        this.authenticationContext = authenticationContext;
        this.auditEventStatus = auditEventStatus;
        this.timestamp = timestamp;
        this.lazyUUID = new Lazy<>();
    }

    public AuthenticationEventImpl(KafkaPrincipal principal,
                                   AuthenticationContext authenticationContext,
                                   AuditEventStatus auditEventStatus) {
        this(principal, authenticationContext, auditEventStatus, Instant.now());
    }

    @Override
    public UUID uuid() {
        return lazyUUID.getOrCompute(UUID::randomUUID);
    }

    @Override
    public Instant timestamp() {
        return timestamp;
    }

    @Override
    public AuditEventType type() {
        return AuditEventType.AUTHENTICATION;
    }

    @Override
    public AuditEventStatus status() {
        return auditEventStatus;
    }

    @Override
    public Map<String, Object> data() {
        return data;
    }

    public void setData(final Map<String, Object> data) {
        this.data = data;
    }

    @Override
    public Optional<KafkaPrincipal> principal() {
        return principal;
    }

    @Override
    public AuthenticationContext authenticationContext() {
        return authenticationContext;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final AuthenticationEventImpl that = (AuthenticationEventImpl) o;
        return Objects.equals(timestamp, that.timestamp) &&
            Objects.equals(lazyUUID, that.lazyUUID) &&
            Objects.equals(principal, that.principal) &&
            Objects.equals(authenticationContext, that.authenticationContext) &&
            auditEventStatus == that.auditEventStatus &&
            Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, lazyUUID, principal, authenticationContext, auditEventStatus, data);
    }

    @Override
    public String toString() {
        return "AuthenticationEventImpl{" +
            "timestamp=" + timestamp +
            ", lazyUUID=" + lazyUUID +
            ", principal=" + principal +
            ", authenticationContext=" + authenticationContext +
            ", auditEventStatus=" + auditEventStatus +
            ", data=" + data +
            '}';
    }
}


