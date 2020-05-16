// (Copyright) [2020 - 2020] Confluent, Inc.

package org.apache.kafka.server.audit;

import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;

public interface AuthorizationEvent extends AuditEvent {

    AuthorizableRequestContext requestContext();

    Action authorizeAction();

    AuthorizationResult authorizationResult();
}
