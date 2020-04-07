// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.Scope;
import java.util.Set;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Interface used by providers of access rules used for authorization.
 * Access rules may be derived from ACLs, RBAC policies etc.
 */
public interface AccessRuleProvider extends Provider {

  /**
   * Returns true if the provided principal is a super user. All operations are authorized
   * for super-users without checking any access rules.
   *
   * @param principal User principal from the Session or the group principal of a group that
   *                  the user belongs to.
   * @param scope Scope of resource being access
   * @return true if super-user or super-group
   */
  boolean isSuperUser(KafkaPrincipal principal, Scope scope);

  /**
   * Returns the first matching access rule for the user and group principals that match the provided
   * resource. If a DENY rule is found for the user or group, the DENY rule is returned. Otherwise
   * one of the matching ALLOW rules is returned.
   *
   * @param sessionPrincipal User principal from the Session
   * @param groupPrincipals List of group principals of the user, which may be empty
   * @param host Client IP address
   * @param action Action being authorized
   * @return Matching rule that includes any deny or allow rule and a boolean that indicates if
   *         there are no rules match the resource.
   */
  AuthorizeRule findRule(KafkaPrincipal sessionPrincipal,
                         Set<KafkaPrincipal> groupPrincipals,
                         String host,
                         Action action);

  /**
   * Returns true if this provider supports DENY rules. If false, this provider's rules are
   * not retrieved if an ALLOW rule was found on another provider.
   * @return Boolean indicating if the provider supports DENY rules.
   */
  boolean mayDeny();
}
