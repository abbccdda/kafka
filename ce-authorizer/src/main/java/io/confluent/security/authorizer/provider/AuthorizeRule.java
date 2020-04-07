// (Copyright) [2020 - 2020] Confluent, Inc.


package io.confluent.security.authorizer.provider;

import io.confluent.security.authorizer.AccessRule;
import java.util.Optional;

/**
 * Matching access rules returned by {@link AccessRuleProvider} for authorization.
 */
public class AuthorizeRule {

  private Optional<AccessRule> denyRule;
  private Optional<AccessRule> allowRule;
  private boolean noResourceAcls;

  public AuthorizeRule() {
    denyRule = Optional.empty();
    allowRule = Optional.empty();
    noResourceAcls = true;
  }

  /**
   * Returns first matching rule with {@link org.apache.kafka.common.acl.AclPermissionType#DENY}
   */
  public Optional<AccessRule> denyRule() {
    return denyRule;
  }

  /**
   * Returns first matching rule with {@link org.apache.kafka.common.acl.AclPermissionType#ALLOW}.
   * ALLOW rules may not be returned if there are DENY rules since search stops if a DENY
   * rule is found.
   */
  public Optional<AccessRule> allowRule() {
    return allowRule;
  }

  /**
   * Returns true if there are no ACLs for the resource.
   * {@link io.confluent.security.authorizer.ConfluentAuthorizerConfig#ALLOW_IF_NO_ACLS_PROP}
   * will be applied if `noResourceAcls` is true.
   */
  public boolean noResourceAcls() {
    return noResourceAcls;
  }

  public void addRuleIfNotExist(AccessRule rule) {
    this.noResourceAcls = false;
    if (!this.denyRule.isPresent()) {
      switch (rule.permissionType()) {
        case DENY:
          this.denyRule = Optional.of(rule);
          break;
        case ALLOW:
          if (!allowRule.isPresent())
            this.allowRule = Optional.of(rule);
          break;
        default:
          throw new IllegalArgumentException("Unsupported access rule permission type " + rule.permissionType());
      }
    }
  }

  public void add(AuthorizeRule other) {
    if (!this.denyRule.isPresent()) {
      other.denyRule.ifPresent(this::addRuleIfNotExist);
      other.allowRule.ifPresent(this::addRuleIfNotExist);
    }
    if (!other.noResourceAcls)
      this.noResourceAcls = false;
  }

  public void noResourceAcls(boolean noResourceAcls) {
    this.noResourceAcls = noResourceAcls;
  }

  public boolean deny() {
    return denyRule.isPresent();
  }

  @Override
  public String toString() {
    return "AuthorizeRule{" +
        "denyRule=" + denyRule +
        ", allowRule=" + allowRule +
        ", noResourceAcls=" + noResourceAcls +
        '}';
  }
}
