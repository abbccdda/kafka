// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import io.confluent.security.authorizer.AuthorizePolicy.AccessRulePolicy;
import io.confluent.security.authorizer.AuthorizePolicy.PolicyType;
import java.util.Objects;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Encapsulates an access rule which may be derived from an ACL or RBAC policy.
 * Operations and resource types are extensible to enable this to be used for
 * authorization in different components.
 */
public class AccessRule {

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
  private final Object sourceMetadata;

  public AccessRule(ResourcePattern resourcePattern,
                    KafkaPrincipal principal,
                    PermissionType permissionType,
                    String host,
                    Operation operation,
                    PolicyType policyType,
                    Object sourceMetadata) {
    this.resourcePattern = Objects.requireNonNull(resourcePattern);
    this.principal = Objects.requireNonNull(principal);
    this.permissionType = Objects.requireNonNull(permissionType);
    this.host = host;
    this.operation = Objects.requireNonNull(operation);
    this.policyType = Objects.requireNonNull(policyType);
    this.sourceMetadata = Objects.requireNonNull(sourceMetadata);
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

  public Object sourceMetadata() {
    return sourceMetadata;
  }

  public AuthorizePolicy toAuthorizePolicy() {
    return new AccessRulePolicy(policyType, sourceMetadata, resourcePattern);
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
    return String.format("%s has %s permission for operation %s on %s from host %s (source: %s: %s)",
        principal, permissionType, operation, resourcePattern, host, policyType, sourceMetadata);
  }
}
