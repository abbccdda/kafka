// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

public interface AuthorizePolicy {

  static NoMatchingRule ALLOW_ON_NO_RULE = new NoMatchingRule(PolicyType.ALLOW_ON_NO_RULE);
  static NoMatchingRule DENY_ON_NO_RULE = new NoMatchingRule(PolicyType.DENY_ON_NO_RULE);
  static NoMatchingRule NO_MATCHING_RULE = new NoMatchingRule(PolicyType.NO_MATCHING_RULE);

  PolicyType policyType();

  enum PolicyType {
    SUPER_USER(true),
    SUPER_GROUP(true),
    ALLOW_ON_NO_RULE(true),
    DENY_ON_NO_RULE(false),
    ALLOW_ACL(true),
    DENY_ACL(false),
    ALLOW_ROLE(true),
    NO_MATCHING_RULE(false),
    UNKNOWN(false);

    private final boolean accessGranted;

    PolicyType(boolean accessGranted) {
      this.accessGranted = accessGranted;
    }

    boolean accessGranted() {
      return accessGranted;
    }
  }

  class SuperUser implements AuthorizePolicy {
    private final PolicyType authorizeType;
    private final KafkaPrincipal principal;

    public SuperUser(PolicyType authorizeType, KafkaPrincipal principal) {
      if (authorizeType != PolicyType.SUPER_USER && authorizeType != PolicyType.SUPER_GROUP)
        throw new IllegalArgumentException("Invalid authorizeType " + authorizeType);
      this.authorizeType = authorizeType;
      this.principal = principal;
    }

    @Override
    public PolicyType policyType() {
      return authorizeType;
    }

    public KafkaPrincipal principal() {
      return principal;
    }

    @Override
    public String toString() {
      return "SuperUser(" +
          "authorizeType=" + authorizeType +
          ", principal=" + principal +
          ')';
    }
  }

  class NoMatchingRule implements AuthorizePolicy {
    private final PolicyType authorizeType;

    public NoMatchingRule(PolicyType authorizeType) {
      if (authorizeType != PolicyType.ALLOW_ON_NO_RULE &&
          authorizeType != PolicyType.DENY_ON_NO_RULE &&
          authorizeType != PolicyType.NO_MATCHING_RULE) {
        throw new IllegalArgumentException("Invalid authorizeType " + authorizeType);
      }
      this.authorizeType = authorizeType;
    }

    @Override
    public PolicyType policyType() {
      return authorizeType;
    }

    @Override
    public String toString() {
      return "NoMatchingRule(" +
          "authorizeType=" + authorizeType +
          ')';
    }
  }

  class AccessRulePolicy implements AuthorizePolicy {
    private final PolicyType authorizeType;
    private final ResourcePattern resourcePattern;
    private final Object sourceMetadata;

    public AccessRulePolicy(PolicyType authorizeType, Object sourceMetadata, ResourcePattern resourcePattern) {
      if (authorizeType != PolicyType.ALLOW_ACL && authorizeType != PolicyType.DENY_ACL && authorizeType != PolicyType.ALLOW_ROLE)
        throw new IllegalArgumentException("Invalid authorizeType " + authorizeType);
      this.authorizeType = authorizeType;
      this.sourceMetadata = sourceMetadata;
      this.resourcePattern = resourcePattern;
    }

    @Override
    public PolicyType policyType() {
      return authorizeType;
    }

    public Object sourceMetadata() {
      return sourceMetadata;
    }

    public ResourcePattern resourcePattern() {
      return resourcePattern;
    }

    @Override
    public String toString() {
      return "AccessRulePolicy(" +
          "authorizeType=" + authorizeType +
          ", resourcePattern=" + resourcePattern +
          ", sourceMetadata=" + sourceMetadata +
          ')';
    }
  }
}