// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.audit;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Lazy;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public class DefaultAuthenticationEvent implements AuthenticationEvent {

    private final Instant timestamp;
    private final Lazy<UUID> lazyUUID;
    private final KafkaPrincipal principal;
    private final AuthenticationContext authenticationContext;
    private final AuditEventStatus auditEventStatus;
    private final AuthenticationException authenticationException;

    private Map<String, Object> data = Collections.emptyMap();

    public DefaultAuthenticationEvent(KafkaPrincipal principal,
                                      AuthenticationContext authenticationContext,
                                      AuditEventStatus auditEventStatus,
                                      AuthenticationException authenticationException,
                                      Instant timestamp) {
        this.principal = principal;
        this.authenticationContext = authenticationContext;
        this.auditEventStatus = auditEventStatus;
        this.timestamp = timestamp;
        this.authenticationException = authenticationException;
        this.lazyUUID = new Lazy<>();
    }

    public DefaultAuthenticationEvent(KafkaPrincipal principal,
                                      AuthenticationContext authenticationContext,
                                      AuditEventStatus auditEventStatus,
                                      AuthenticationException authenticationException) {
        this(principal, authenticationContext, auditEventStatus, authenticationException, Instant.now());
    }

    public DefaultAuthenticationEvent(KafkaPrincipal principal,
                                      AuthenticationContext authenticationContext,
                                      AuthenticationException authenticationException) {
        this(principal, authenticationContext, getAuditEventStatus(authenticationException), authenticationException, Instant.now());
    }

    public DefaultAuthenticationEvent(final KafkaPrincipal principal,
                                      final AuthenticationContext authenticationContext,
                                      final AuditEventStatus auditEventStatus) {
        this(principal, authenticationContext, auditEventStatus, null, Instant.now());

    }


    private static AuditEventStatus getAuditEventStatus(final AuthenticationException authenticationException) {
        return authenticationException.errorInfo().auditEventStatus();
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
        return Optional.ofNullable(principal);
    }

    @Override
    public AuthenticationContext authenticationContext() {
        return authenticationContext;
    }

    @Override
    public Optional<AuthenticationException> authenticationException() {
        return Optional.ofNullable(authenticationException);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DefaultAuthenticationEvent that = (DefaultAuthenticationEvent) o;
        return Objects.equals(timestamp, that.timestamp) &&
            Objects.equals(lazyUUID, that.lazyUUID) &&
            Objects.equals(principal, that.principal) &&
            Objects.equals(authenticationContext, that.authenticationContext) &&
            auditEventStatus == that.auditEventStatus &&
            Objects.equals(authenticationException, that.authenticationException) &&
            Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, lazyUUID, principal, authenticationContext, auditEventStatus, authenticationException, data);
    }

    @Override
    public String toString() {
        return "DefaultAuthenticationEvent{" +
            "timestamp=" + timestamp +
            ", lazyUUID=" + lazyUUID +
            ", principal=" + principal +
            ", authenticationContext=" + authenticationContext +
            ", auditEventStatus=" + auditEventStatus +
            ", authenticationException=" + authenticationException +
            ", data=" + data +
            '}';
    }
}


