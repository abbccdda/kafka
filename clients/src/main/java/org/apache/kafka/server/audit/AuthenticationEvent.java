// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.audit;

import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Optional;

public interface AuthenticationEvent extends AuditEvent {

    Optional<KafkaPrincipal> principal();

    AuthenticationContext authenticationContext();

}
