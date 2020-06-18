/*
 * Copyright [2020 - 2020] Confluent Inc.
 */

package org.apache.kafka.jmh.audit;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.audit.AuditEventStatus;

class AuthenticationEventArguments {
    public final KafkaPrincipal principal;
    public final AuthenticationContext authenticationContext;
    public final AuditEventStatus auditEventStatus;
    public final AuthenticationException authenticationException;

    AuthenticationEventArguments(
        final KafkaPrincipal principal,
        final AuthenticationContext authenticationContext,
        final AuditEventStatus auditEventStatus,
        final AuthenticationException authenticationException) {
        this.principal = principal;
        this.authenticationContext = authenticationContext;
        this.auditEventStatus = auditEventStatus;
        this.authenticationException = authenticationException;
    }
}
