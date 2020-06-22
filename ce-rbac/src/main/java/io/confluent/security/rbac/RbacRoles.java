// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.utils.JsonMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The RBAC policy definition. Roles are currently statically defined
 * in the JSON file `rbac_policy.json`.
 */
public class RbacRoles {

  private static final String DEFAULT_POLICY_FILE = "default_rbac_roles.json";
  private static final String CLOUD_POLICY_FILE = "cloud_rbac_roles.json";

  private final List<String> bindingScopes;
  private final Map<String, Role> roles;

  @JsonCreator
  public RbacRoles(@JsonProperty("roles") List<Role> roles,
                   @JsonProperty("bindingScopes") List<String> bindingScopes) {
    validateBindingScopes(bindingScopes);
    this.bindingScopes = Collections.unmodifiableList(bindingScopes);
    this.roles = new HashMap<>();
    roles.forEach(this::addRole);
  }

  private void validateBindingScopes(List<String> bindingScopes) {
    if (!Scope.CLUSTER_BINDING_SCOPE.equals(bindingScopes.get(0))) {
      throw new InvalidRoleDefinitionException(
              "first binding scope must be '" + Scope.CLUSTER_BINDING_SCOPE + "'");
    }
    // It is allowed for the root binding scope to not be present, but if it's present,
    // it must be last
    if (bindingScopes.contains(Scope.ROOT_BINDING_SCOPE) &&
            bindingScopes.indexOf(Scope.ROOT_BINDING_SCOPE) != bindingScopes.size() - 1) {
      throw new InvalidRoleDefinitionException(
              "binding scope '" + Scope.ROOT_BINDING_SCOPE + "' must be last");
    }
    Set<String> seen = new HashSet<>();
    for (String bindingScope : bindingScopes) {
      if (!Scope.SCOPE_TYPE_PATTERN.matcher(bindingScope).matches()) {
        throw new InvalidRoleDefinitionException(
                "bindingScopes may only contain letters and '-': '" + bindingScope + "'");
      }
      if (seen.contains(bindingScope)) {
        throw new InvalidRoleDefinitionException(
                "bindingScopes may not be repeated: '" + bindingScope + "'");
      }
      seen.add(bindingScope);
    }
  }

  public Role role(String roleName) {
    return roles.get(roleName);
  }

  public Collection<Role> roles() {
    return roles.values();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RbacRoles)) {
      return false;
    }

    RbacRoles that = (RbacRoles) o;
    return Objects.equals(roles, that.roles);
  }

  @Override
  public int hashCode() {
    return Objects.hash(roles);
  }

  void addRole(Role role) {
    for (AccessPolicy accessPolicy : role.accessPolicies().values()) {
      accessPolicy.allowedOperations().forEach(resourceOp -> {
        if (!bindingScopes.contains(accessPolicy.bindingScope()))
          throw new InvalidRoleDefinitionException(
                  "Unknown binding scope '" + accessPolicy.bindingScope() + "' defined for " + role);
        if (resourceOp.resourceType() == null || resourceOp.resourceType().isEmpty())
          throw new InvalidRoleDefinitionException(
                  "Resource type not specified in role definition ops for " + role);
        resourceOp.operations().forEach(op -> {
          if (op.isEmpty())
            throw new InvalidRoleDefinitionException(
                    "Operation name not specified in role definition ops for " + role);
        });
      });
    }
    // This must be done here because the Role itself doesn't know the order of scopes
    role.setMostSpecificBindingScope(mostSpecificBindingScope(role));
    this.roles.put(role.name(), role);
  }

  public static RbacRoles loadDefaultPolicy(boolean isConfluentCloud)
          throws InvalidRoleDefinitionException {
    if (isConfluentCloud) {
      return load(RbacRoles.class.getClassLoader(), CLOUD_POLICY_FILE);
    }
    return load(RbacRoles.class.getClassLoader(), DEFAULT_POLICY_FILE);
  }

  public static RbacRoles load(ClassLoader classLoader, String policyResourceName)
      throws InvalidRoleDefinitionException {
    try {
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream(policyResourceName)))) {
        return JsonMapper.objectMapper().readValue(reader, RbacRoles.class);
      }
    } catch (IOException e) {
      throw new InvalidRoleDefinitionException("RBAC policies could not be loaded from " + policyResourceName, e);
    }
  }

  private String mostSpecificBindingScope(Role role) {
    Set<String> roleBindingScopes = role.bindingScopes();
    for (String bindingScope : bindingScopes) {
      if (roleBindingScopes.contains(bindingScope)) {
        return bindingScope;
      }
    }
    return null;
  }
}
