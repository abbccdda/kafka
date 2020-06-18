// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.audit;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.Optional;

/**
 * The {@code AuthenticationEvent} interface is used to post authentication audit events.
 */
@InterfaceStability.Evolving
public interface AuthenticationEvent extends AuditEvent {

    /**
     * Returns the authenticated {@code KafkaPrincipal} responsible for the event. Returns {@code Option.empty()} if the
     * this is Authentication failure event.
     * @return the Kafka Principal
     */
    Optional<KafkaPrincipal> principal();

    /**
     * Returns {@code AuthenticationContext} from the authenticating session. In case of authentication errors, some
     * of the  {@code AuthenticationContext} implementation class methods may throw error.
     * @return the AuthenticationContext
     */
    AuthenticationContext authenticationContext();

    /**
     * Gets an {@code AuthenticationException} object from which additional audit information can be obtained in case
     * authentication failure errors. Returns {@code Option.empty()} if the there is no Authentication exception.
     * @return the AuthenticationException
     */
    Optional<AuthenticationException> authenticationException();
}
