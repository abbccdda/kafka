// (Copyright) [2020 - 2020] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.Scope;
import org.apache.kafka.common.utils.Lazy;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.apache.kafka.server.audit.AuditEventType;
import org.apache.kafka.server.audit.AuthorizationEvent;
import org.apache.kafka.server.authorizer.AuthorizationResult;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class ConfluentAuthorizationEvent implements AuthorizationEvent {

    private final Instant timestamp;
    private final Scope sourceScope;
    private final RequestContext requestContext;
    private final Action action;
    private final AuthorizeResult authorizeResult;
    private final AuthorizePolicy authorizePolicy;
    private final Lazy<UUID> lazyUUID;

    private Map<String, Object> data = new HashMap<>();

    public ConfluentAuthorizationEvent(Scope sourceScope,
                                       RequestContext requestContext,
                                       Action action,
                                       AuthorizeResult authorizationResult,
                                       AuthorizePolicy authorizePolicy) {
        this.sourceScope = sourceScope;
        this.requestContext = requestContext;
        this.action = action;
        this.authorizeResult = authorizationResult;
        this.authorizePolicy = authorizePolicy;
        this.timestamp = Instant.now();
        this.lazyUUID = new Lazy<>();
    }

    public Scope sourceScope() {
        return sourceScope;
    }

    @Override
    public RequestContext requestContext() {
        return requestContext;
    }

    public Action action() {
        return action;
    }

    public AuthorizeResult authorizeResult() {
        return authorizeResult;
    }

    public AuthorizePolicy authorizePolicy() {
        return authorizePolicy;
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
        return AuditEventType.AUTHORIZATION;
    }

    @Override
    public AuditEventStatus status() {
        if (authorizeResult == AuthorizeResult.ALLOWED)
            return AuditEventStatus.SUCCESS;
        else
            return AuditEventStatus.FAILURE;
    }

    @Override
    public Map<String, Object> data() {
        return data;
    }

    public void setData(final Map<String, Object> data) {
        this.data = data;
    }

    //Implement AuthorizationEvent interface methods

    @Override
    public org.apache.kafka.server.authorizer.Action authorizeAction() {
        return action().toKafkaAction();
    }

    @Override
    public AuthorizationResult authorizationResult() {
        if (authorizeResult == AuthorizeResult.ALLOWED)
            return AuthorizationResult.ALLOWED;
        else
            return AuthorizationResult.DENIED;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final ConfluentAuthorizationEvent that = (ConfluentAuthorizationEvent) o;
        return Objects.equals(timestamp, that.timestamp) &&
            Objects.equals(sourceScope, that.sourceScope) &&
            Objects.equals(requestContext, that.requestContext) &&
            Objects.equals(action, that.action) &&
            authorizeResult == that.authorizeResult &&
            Objects.equals(authorizePolicy, that.authorizePolicy) &&
            Objects.equals(lazyUUID, that.lazyUUID) &&
            Objects.equals(data, that.data);
    }

    @Override
    public int hashCode() {
        return Objects.hash(timestamp, sourceScope, requestContext, action, authorizeResult, authorizePolicy, lazyUUID, data);
    }

    @Override
    public String toString() {
        return "ConfluentAuthorizationEvent{" +
            "timestamp=" + timestamp +
            ", sourceScope=" + sourceScope +
            ", requestContext=" + requestContext +
            ", action=" + action +
            ", authorizeResult=" + authorizeResult +
            ", authorizePolicy=" + authorizePolicy +
            ", lazyUUID=" + lazyUUID +
            ", data=" + data +
            '}';
    }
}
