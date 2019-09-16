// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import io.confluent.security.authorizer.AuthorizePolicy;

public class RbacAuthorizePolicy implements AuthorizePolicy {
  private final RoleBinding roleBinding;

  public RbacAuthorizePolicy(RoleBinding roleBinding) {
    this.roleBinding = roleBinding;
  }

  @Override
  public PolicyType policyType() {
    return PolicyType.ALLOW_ROLE;
  }

  public RoleBinding roleBinding() {
    return roleBinding;
  }
}
