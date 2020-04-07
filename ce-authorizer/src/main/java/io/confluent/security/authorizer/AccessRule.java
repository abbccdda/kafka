// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Encapsulates an access rule which may be derived from an ACL or RBAC policy.
 * Operations and resource types are extensible to enable this to be used for
 * authorization in different components.
 */
public abstract class AccessRule implements AuthorizePolicy {

  public static final String ALL_HOSTS = "*";
  public static final KafkaPrincipal WILDCARD_USER_PRINCIPAL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*");
  public static final String GROUP_PRINCIPAL_TYPE = "Group";
  public static final KafkaPrincipal WILDCARD_GROUP_PRINCIPAL = new KafkaPrincipal(GROUP_PRINCIPAL_TYPE, "*");

  private final ResourcePattern resourcePattern;
  private final KafkaPrincipal principal;
  private final PermissionType permissionType;
  private final String host;
  private final Operation operation;
  private final PolicyType policyType;

  public AccessRule(ResourcePattern resourcePattern,
                    KafkaPrincipal principal,
                    PermissionType permissionType,
                    String host,
                    Operation operation,
                    PolicyType policyType) {
    this.resourcePattern = Objects.requireNonNull(resourcePattern);
    this.principal = Objects.requireNonNull(principal);
    this.permissionType = Objects.requireNonNull(permissionType);
    this.host = host;
    this.operation = Objects.requireNonNull(operation);
    this.policyType = Objects.requireNonNull(policyType);
  }

  public ResourcePattern resourcePattern() {
    return resourcePattern;
  }

  public KafkaPrincipal principal() {
    return principal;
  }

  public PermissionType permissionType() {
    return permissionType;
  }

  public String host() {
    return host;
  }

  public Operation operation() {
    return operation;
  }

  public PolicyType policyType() {
    return policyType;
  }

  public boolean matches(Set<KafkaPrincipal> matchingPrincipals,
                         String host,
                         Operation requestedOperation,
                         PermissionType permissionType) {
    return matches(this.principal, this.host, this.operation, this.permissionType,
        matchingPrincipals, host, requestedOperation, permissionType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AccessRule)) {
      return false;
    }

    AccessRule that = (AccessRule) o;
    return Objects.equals(this.resourcePattern, that.resourcePattern) &&
        Objects.equals(this.principal, that.principal) &&
        Objects.equals(this.permissionType, that.permissionType) &&
        Objects.equals(this.host, that.host) &&
        Objects.equals(this.operation, that.operation) &&
        Objects.equals(this.policyType, that.policyType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(resourcePattern, principal, permissionType, host, operation, policyType);
  }

  @Override
  public String toString() {
    return String.format("%s has %s permission for operation %s on %s from host %s (source: %s)",
        principal, permissionType, operation, resourcePattern, host, policyType);
  }

  public static Set<KafkaPrincipal> matchingPrincipals(KafkaPrincipal userPrincipal,
                                                       Collection<KafkaPrincipal> groupPrincipals,
                                                       KafkaPrincipal wildcardUserPrincipal,
                                                       KafkaPrincipal wildcardGroupPrincipal) {
    HashSet<KafkaPrincipal> principals = new HashSet<>(groupPrincipals.size() + 4);
    principals.addAll(groupPrincipals);
    principals.add(userPrincipal);
    if (wildcardUserPrincipal != null)
      principals.add(wildcardUserPrincipal);
    if (wildcardGroupPrincipal != null && !groupPrincipals.isEmpty())
      principals.add(wildcardGroupPrincipal);
    return principals;
  }

  public static boolean matches(KafkaPrincipal rulePrincipal,
                                String ruleHost,
                                Operation ruleOperation,
                                PermissionType rulePermissionType,
                                Set<KafkaPrincipal> matchingPrincipals,
                                String host,
                                Operation requestedOperation,
                                PermissionType permissionType) {
    return rulePermissionType == permissionType &&
        matchingPrincipals.contains(rulePrincipal) &&
        ruleOperation.matches(requestedOperation, permissionType) &&
        (ruleHost.equals(AccessRule.ALL_HOSTS) || ruleHost.equals(host));
  }
}
