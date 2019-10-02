// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import java.util.Map;
import org.apache.kafka.common.Reconfigurable;

/**
 * Interface used by audit log provider that logs authorization events.
 */
public interface AuditLogProvider extends Provider, Reconfigurable {

  /**
   * Generates an audit log entry with the provided data.
   *
   * @param requestContext  Request context that contains details of the request that was being
   *                        authorized. This includes the user principal.
   * @param action          The action that was being authorized including resource and operation.
   * @param authorizeResult Result of the authorization indicating if access was granted.
   * @param authorizePolicy Details of the authorization policy that granted or denied access.
   *                        This includes any ACL/Role binding that produced the result.
   */
  void logAuthorization(RequestContext requestContext,
           Action action,
           AuthorizeResult authorizeResult,
           AuthorizePolicy authorizePolicy);

  /**
   * Returns true if minimal configs of this provider are included in the provided configs.
   */
  boolean providerConfigured(Map<String, ?> configs);
}
