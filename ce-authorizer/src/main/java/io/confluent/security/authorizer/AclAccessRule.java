/*
 * Copyright [2019 - 2020] Confluent Inc.
 */
package io.confluent.security.authorizer;

import java.util.Objects;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class AclAccessRule extends AccessRule {

  private final AclBinding aclBinding;

  public AclAccessRule(ResourcePattern resourcePattern,
      KafkaPrincipal principal,
      PermissionType permissionType, String host,
      Operation operation, PolicyType policyType, AclBinding aclBinding) {
    super(resourcePattern, principal, permissionType, host, operation, policyType);
    this.aclBinding = Objects.requireNonNull(aclBinding);
  }

  public AclBinding aclBinding() {
    return aclBinding;
  }

  @Override
  public String toString() {
    return String.format("%s has %s permission for operation %s on %s from host %s (source: %s: %s)",
        principal(), permissionType(), operation(), resourcePattern(), host(), policyType(), aclBinding);
  }
}
