/*
 * Copyright [2019 - 2020] Confluent Inc.
 */
package io.confluent.security.rbac;

import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.PermissionType;
import io.confluent.security.authorizer.ResourcePattern;
import java.util.Objects;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class RbacAccessRule extends AccessRule {

  private final RoleBinding roleBinding;

  public RbacAccessRule(ResourcePattern resourcePattern,
      KafkaPrincipal principal,
      PermissionType permissionType, String host,
      Operation operation, PolicyType policyType, RoleBinding roleBinding) {
    super(resourcePattern, principal, permissionType, host, operation, policyType);
    this.roleBinding = Objects.requireNonNull(roleBinding);
  }

  public RoleBinding roleBinding() {
    return roleBinding;
  }

  @Override
  public String toString() {
    return String.format("%s has %s permission for operation %s on %s from host %s (source: %s: %s)",
        principal(), permissionType(), operation(), resourcePattern(), host(), policyType(), roleBinding);
  }
}
