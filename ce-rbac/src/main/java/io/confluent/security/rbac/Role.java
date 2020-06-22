// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Role definition including access policy.
 */
public class Role {

  private final String name;
  private final Map<String, AccessPolicy> accessPolicies;
  private String mostSpecificBindingScope = null;

  @JsonCreator
  public Role(@JsonProperty("name") String name,
              @JsonProperty("policy") AccessPolicy accessPolicy,
              @JsonProperty("policies") List<AccessPolicy> accessPolicies) {
    if (name == null || name.isEmpty())
      throw new IllegalArgumentException("Role name must be non-empty");
    this.name = name;
    HashMap<String, AccessPolicy> policies = new HashMap<>();
    if (accessPolicy != null) {
      if (accessPolicies != null) {
        throw new InvalidRoleDefinitionException(
                "role must not define both 'accessPolicy' and 'accessPolicies'");
      }
      policies.put(accessPolicy.bindingScope(), accessPolicy);
    }
    if (accessPolicies != null) {
      for (AccessPolicy policy : accessPolicies) {
        String bindingScope = policy.bindingScope();
        if (policies.containsKey(bindingScope)) {
          throw new InvalidRoleDefinitionException(
                  "each scope level may only have one policy: " + bindingScope);
        }
        policies.put(bindingScope, policy);
      }
    }
    if (policies.isEmpty()) {
      throw new InvalidRoleDefinitionException("at least one access policy must be supplied");
    }
    this.accessPolicies = Collections.unmodifiableMap(policies);
  }

  @JsonProperty
  public String name() {
    return name;
  }

  @JsonProperty("policies")
  public Map<String, AccessPolicy> accessPolicies() {
    return accessPolicies;
  }

  public Set<String> bindingScopes() {
    return accessPolicies.keySet();
  }

  public boolean bindWithResource() {
    return accessPolicies.values().stream().anyMatch(AccessPolicy::bindWithResource);
  }

  /*
   * This can only be known in the context of an ordered list of defined bindingScopes,
   * so it is set when this Role is added to an RbacRoles
   */
  void setMostSpecificBindingScope(String bindingScope) {
    this.mostSpecificBindingScope = bindingScope;
  }

  public String mostSpecificBindingScope() {
    return this.mostSpecificBindingScope;
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
