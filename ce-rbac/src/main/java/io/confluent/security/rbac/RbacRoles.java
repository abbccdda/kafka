// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.ScopeType;
import io.confluent.security.authorizer.utils.JsonMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The RBAC policy definition. Roles are currently statically defined
 * in the JSON file `rbac_policy.json`.
 */
public class RbacRoles {

  private static final String DEFAULT_POLICY_FILE = "default_rbac_roles.json";
  private static final String CLOUD_POLICY_FILE = "cloud_rbac_roles.json";

  private final Map<String, Role> roles;

  @JsonCreator
  public RbacRoles(@JsonProperty("roles") List<Role> roles) {
    this.roles = new HashMap<>();
    roles.forEach(this::addRole);
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
        if (accessPolicy.scopeType() == ScopeType.UNKNOWN)
          throw new InvalidRoleDefinitionException("Unknown scope type defined for " + role);
        if (resourceOp.resourceType() == null || resourceOp.resourceType().isEmpty())
          throw new InvalidRoleDefinitionException("Resource type not specified in role definition ops for " + role);
        resourceOp.operations().forEach(op -> {
          if (op.isEmpty())
            throw new InvalidRoleDefinitionException("Operation name not specified in role definition ops for " + role);
        });
      });
    }
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
}
