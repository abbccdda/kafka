// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.audit;

import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

/**
 * The {@code AuthorizationEvent} interface is used to post authorization audit events.
 */
@InterfaceStability.Evolving
public interface AuthorizationEvent extends AuditEvent {

    /**
     * Returns {@code AuthorizableRequestContext} from the authenticated session/request.
     * @return the AuthorizableRequestContext
     */
    AuthorizableRequestContext requestContext();

    /**
     * Returns {@code Action} of the authorization event.
     * @return the Action
     */
    Action authorizeAction();

    /**
     * Returns {@code AuthorizationResult} of the authorization event.
     * @return the AuthorizationResult
     */
    AuthorizationResult authorizationResult();
}
