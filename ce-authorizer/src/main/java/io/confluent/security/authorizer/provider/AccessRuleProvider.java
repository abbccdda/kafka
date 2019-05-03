// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import java.util.Set;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Interface used by providers of access rules used for authorization.
 * Access rules may be derived from ACLs, RBAC policies etc.
 */
public interface AccessRuleProvider extends Provider {

  /**
   * Configures authorization scope for this provider, which indicates the scope of
   * authorization metadata stored by this provider if this provider uses a centralized metadata
   * service. For a broker not hosting a metadata service for authorization in other components,
   * scope is the broker's cluster id. For a broker hosting metadata service, scope is the empty
   * root scope. Scope is not used by cluster-specific providers (e.g. ACLs).
   * @param scope metadata scope
   */
  default void configureScope(Scope scope) {
  }

  /**
   * Returns true if either the session's user principal or one of the provided group
   * principals is a super user. All operations are authorized for super-users without
   * checking any access rules.
   *
   * @param sessionPrincipal User principal from the Session
   * @param groupPrincipals List of group principals of the user, which may be empty
   * @param scope Scope of resource being access
   * @return true if super-user or super-group
   */
  boolean isSuperUser(KafkaPrincipal sessionPrincipal, Set<KafkaPrincipal> groupPrincipals, Scope scope);

  /**
   * Returns the set of access rules for the user and group principals that match the provided
   * resource.
   *
   * @param sessionPrincipal User principal from the Session
   * @param groupPrincipals List of group principals of the user, which may be empty
   * @param scope Scope of resource
   * @param resource Resource being accessed
   * @return Set of matching rules
   */
  Set<AccessRule> accessRules(KafkaPrincipal sessionPrincipal,
                              Set<KafkaPrincipal> groupPrincipals,
                              Scope scope,
                              ResourcePattern resource);

  /**
   * Returns true if this provider supports DENY rules. If false, this provider's rules are
   * not retrieved if an ALLOW rule was found on another provider.
   * @return Boolean indicating if the provider supports DENY rules.
   */
  boolean mayDeny();
}
