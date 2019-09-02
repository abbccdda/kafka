// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.RoleBinding;
import io.confluent.security.rbac.RoleBindingFilter;
import io.confluent.security.rbac.UserMetadata;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Cache containing authorization and authentication metadata. This is obtained from
 * a Kafka metadata topic.
 */
public interface AuthCache {

  /**
   * Returns the groups of the provided user principal.
   * @param userPrincipal User principal
   * @return Set of group principals of the user, which may be empty
   */
  Set<KafkaPrincipal> groups(KafkaPrincipal userPrincipal);

  /**
   * Returns the RBAC rules corresponding to the provided principals that match
   * the specified resource.
   *
   * @param resourceScope Scope of the resource
   * @param resource Resource pattern to match
   * @param userPrincipal User principal
   * @param groupPrincipals Set of group principals of the user
   * @return Set of access rules that match the principals and resource
   */
  Set<AccessRule> rbacRules(Scope resourceScope,
                            ResourcePattern resource,
                            KafkaPrincipal userPrincipal,
                            Collection<KafkaPrincipal> groupPrincipals);


  /**
   * Returns the role bindings at the specified scope. Note that roles bindings of
   * parent scopes are not returned. The returned collection may be empty.
   *
   * @param scope Scope for which role bindings are requested.
   * @return Set of roles currently assigned at the specified scope
   */
  Set<RoleBinding> rbacRoleBindings(Scope scope);

  /**
   * Returns role bindings that match the specified filter.
   *
   * @param filter The filter used for matching role bindings
   * @return Set of role bindings that match the filter
   */
  Set<RoleBinding> rbacRoleBindings(RoleBindingFilter filter);

  /**
   * Returns metadata for the specified user principal if available or null if user is not known.
   *
   * @param userPrincipal KafkaPrincipal of user
   * @return user metadata including group membership
   */
  UserMetadata userMetadata(KafkaPrincipal userPrincipal);

  /**
   * Returns user metadata for all users.
   */
  Map<KafkaPrincipal, UserMetadata> users();

  /**
   * Returns the root scope of this cache. The cache discards entries with scope that is
   * not contained within the root scope.
   * @return root scope of cache
   */
  Scope rootScope();

  /**
   * Returns the RBAC role definitions associated with this cache.
   * @return RBAC role definitions
   */
  RbacRoles rbacRoles();


  /**
   * Returns the ACL rules corresponding to the provided principal that match
   * the specified resource.
   *
   * @param resourceScope Scope of the resource
   * @param resource Resource pattern to match
   * @param userPrincipal User principal
   * @param groupPrincipals Set of group principals of the user
   * @return Set of access rules that match the principals and resource
   */
  Set<AccessRule> aclRules(Scope resourceScope,
                           ResourcePattern resource,
                           KafkaPrincipal userPrincipal,
                           Collection<KafkaPrincipal> groupPrincipals);

  /**
   * Returns the ACL rules for all resources of given scope
   *
   * @param scope Scope of the resources
   * @return ACL rules for all resources of given scope
   */
  Map<ResourcePattern, Set<AccessRule>> aclRules(Scope scope);

  /**
   * Returns ACL bindings which match the provided filter.
   *
   * @param scope Scope of the acl search.
   * @param aclBindingFilter AclBindingFilter to match
   * @param resourceAccess predicate to check resource access permission
   * @return Set of ACL bindings which match the provided aclBindingFilter
   */
  Collection<AclBinding> aclBindings(Scope scope,
                                     AclBindingFilter aclBindingFilter,
                                     Predicate<ResourcePattern> resourceAccess);
}
