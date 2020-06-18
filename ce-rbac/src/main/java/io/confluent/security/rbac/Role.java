// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.ScopeType;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Role definition including access policy.
 */
public class Role {

  private final String name;
  private final Map<ScopeType, AccessPolicy> accessPolicies;

  @JsonCreator
  public Role(@JsonProperty("name") String name,
              @JsonProperty("policy") AccessPolicy accessPolicy,
              @JsonProperty("policies") List<AccessPolicy> accessPolicies) {
    if (name == null || name.isEmpty())
      throw new IllegalArgumentException("Role name must be non-empty");
    this.name = name;
    EnumMap<ScopeType, AccessPolicy> policies = new EnumMap<>(ScopeType.class);
    if (accessPolicy != null) {
      if (accessPolicies != null) {
        throw new InvalidRoleDefinitionException(
                "role must not define both 'accessPolicy' and 'accessPolicies'");
      }
      policies.put(accessPolicy.scopeType(), accessPolicy);
    }
    if (accessPolicies != null) {
      for (AccessPolicy policy : accessPolicies) {
        ScopeType scopeType = policy.scopeType();
        if (policies.containsKey(scopeType)) {
          throw new InvalidRoleDefinitionException(
                  "each scope type may only have one policy: " + scopeType);
        }
        policies.put(scopeType, policy);
      }
    }
    if (policies.isEmpty()) {
      throw new InvalidRoleDefinitionException("at least one access policy must be supplied");
    }
    this.accessPolicies = Collections.unmodifiableMap(policies);
  }

  public String name() {
    return name;
  }

  public Map<ScopeType, AccessPolicy> accessPolicies() {
    return accessPolicies;
  }

  public Set<ScopeType> scopeTypes() {
    return accessPolicies.keySet();
  }

  public boolean hasResourceScope() {
    return accessPolicies.containsKey(ScopeType.RESOURCE);
  }

  public ScopeType mostSpecificScopeType() {
    // use the natural ordering of the ScopeType enum
    return accessPolicies.keySet().stream().sorted().findFirst().orElse(ScopeType.UNKNOWN);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Role)) {
      return false;
    }

    Role that = (Role) o;
    return Objects.equals(name, that.name) && Objects.equals(accessPolicies, that.accessPolicies);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, accessPolicies);
  }

  @Override
  public String toString() {
    return name;
  }
}
