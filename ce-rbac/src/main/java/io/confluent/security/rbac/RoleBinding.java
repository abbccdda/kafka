// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Role binding that assigns a role to a user or group principal at a specified scope.
 * The binding may be scoped to specific resources by specifying a collection of resources.
 */
public class RoleBinding {

  private final KafkaPrincipal principal;
  private final Collection<ResourcePattern> resources;
  private final String role;
  private final Scope scope;

  @JsonCreator
  public RoleBinding(@JsonProperty("principal") KafkaPrincipal principal,
                     @JsonProperty("role") String role,
                     @JsonProperty("scope") Scope scope,
                     @JsonProperty("resources") Collection<ResourcePattern> resources) {
    this.principal = Objects.requireNonNull(principal, "principal must not be null");
    if (role == null || role.isEmpty())
      throw new IllegalArgumentException("Role must be non-empty for role binding");
    this.role = role;
    if (scope == null || scope.clusters().isEmpty())
      throw new IllegalArgumentException("Scope must be non-empty for role binding");
    this.scope = scope;
    this.resources = resources == null ? Collections.emptySet() :
        Collections.unmodifiableSet(new HashSet<>(resources));
  }

  @JsonProperty
  public KafkaPrincipal principal() {
    return principal;
  }

  @JsonProperty
  public String role() {
    return role;
  }

  @JsonProperty
  public Scope scope() {
    return scope;
  }

  @JsonProperty
  public Collection<ResourcePattern> resources() {
    return resources;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RoleBinding)) {
      return false;
    }

    RoleBinding that = (RoleBinding) o;
    return Objects.equals(principal, that.principal) &&
        Objects.equals(role, that.role) &&
        Objects.equals(scope, that.scope) &&
        Objects.equals(resources, that.resources);
  }

  @Override
  public int hashCode() {
    return Objects.hash(principal, role, scope, resources);
  }

  @Override
  public String toString() {
    return "RoleBinding(" +
        "principal=" + principal +
        ", role='" + role + '\'' +
        ", scope='" + scope + '\'' +
        ", resources=" + resources +
        ')';
  }
}
