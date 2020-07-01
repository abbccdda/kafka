// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.audit;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * The {@code AuthenticationErrorInfo} class captures the additional authentication error details. This is used in
 * {@code AuthenticationException}. This is used for internal audit logging and should not be used in responses
 * to client requests.
 */
public class AuthenticationErrorInfo {
    public final static AuthenticationErrorInfo UNAUTHENTICATED_USER_ERROR =
            new AuthenticationErrorInfo(AuditEventStatus.UNAUTHENTICATED);

    public final static AuthenticationErrorInfo UNKNOWN_USER_ERROR =
            new AuthenticationErrorInfo(AuditEventStatus.UNKNOWN_USER_DENIED);

    private final AuditEventStatus auditEventStatus;
    private final String errorMessage;
    private final String identifier;
    private final String clusterId;
    private Map<String, String> saslExtensions = Collections.emptyMap();

    public AuthenticationErrorInfo(
            final AuditEventStatus auditEventStatus, final String errorMessage,
            final String identifier, final String clusterId) {
        this.auditEventStatus = Objects.requireNonNull(auditEventStatus);
        this.errorMessage = Objects.toString(errorMessage, "");
        this.identifier = Objects.toString(identifier, "");
        this.clusterId = Objects.toString(clusterId, "");
    }

    public AuthenticationErrorInfo(final AuditEventStatus auditEventStatus) {
        this(auditEventStatus, "", "", "");
    }

    /**
     * Returns the audit event status
     *
     * @return the AuditEventStatus
     */
    public AuditEventStatus auditEventStatus() {
        return auditEventStatus;
    }

    /**
     * Returns additional errorMessage related to the failure. This message is for internal audit logging and
     * this should not be returned to clients.
     *
     * @return the errorMessage
     */
    public String errorMessage() {
        return errorMessage;
    }

    /**
     * Returns the identifier used for authentication. This will be {@code SaslServer.getAuthorizationID()}
     * for SASL authentication and SSLSession's peerPrincipal of the remote host for SSL.
     *
     * @return the identifier
     */
    public String identifier() {
        return identifier;
    }

    /**
     * Returns the cluster Id
     *
     * @return the cluster ID
     */
    public String clusterId() {
        return clusterId;
    }

    /**
     * Returns a map of the SASL extension names and their values
     */
    public Map<String, String> saslExtensions() {
        return saslExtensions;
    }

    public void saslExtensions(final Map<String, String> saslExtensions) {
        this.saslExtensions = Objects.requireNonNull(saslExtensions);
    }

    @Override
    public String toString() {
        return "AuthenticationErrorInfo{" +
                "auditEventStatus=" + auditEventStatus +
                ", errorMessage='" + errorMessage + '\'' +
                ", identifier='" + identifier + '\'' +
                ", clusterId='" + clusterId + '\'' +
                ", saslExtensions=" + saslExtensions +
                '}';
    }
}
