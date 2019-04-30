// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourcePatternFilter;
import io.confluent.security.authorizer.Scope;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

/**
 * Role binding filter that can be used to match role bindings.
 */
public class RoleBindingFilter {

  private final KafkaPrincipal principal;
  private final ResourcePatternFilter resourceFilter;
  private final String role;
  private final Scope scope;

  /**
   * Filter used to match role bindings for describing or deleting matching role bindings.
   * Filter may match some resources within a role binding for a principal. Returns a role binding
   * with resources that match the filter. If binding does not match or if there are no matching
   * resources, null is returned.
   *
   * @param principal Kafka principal to match, null to match all principals
   * @param role RBAC role to match, null to match all roles
   * @param scope RBAC scope to match, null to match all scopes
   * @param resourceFilter Resource filter to match resources, null to match all resources
   *
   */
  @JsonCreator
  public RoleBindingFilter(@JsonProperty("principal") KafkaPrincipal principal,
                           @JsonProperty("role") String role,
                           @JsonProperty("scope") Scope scope,
                           @JsonProperty("resourceFilter") ResourcePatternFilter resourceFilter) {
    this.principal = principal;
    this.role = role;
    this.scope = scope;
    this.resourceFilter = resourceFilter;
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
  public ResourcePatternFilter resourceFilter() {
    return resourceFilter;
  }

  public RoleBinding matchingBinding(RoleBinding roleBinding, boolean matchResource) {
    if (principal != null && !principal.equals(roleBinding.principal()))
      return null;
    if (role != null && !role.equals(roleBinding.role()))
      return null;
    if (scope != null && !scope.containsScope(roleBinding.scope()))
      return null;
    if (resourceFilter == null || !matchResource)
      return roleBinding;

    Set<ResourcePattern> roleResources = new HashSet<>(roleBinding.resources());
    roleResources.removeIf(resource -> !resourceFilter.matches(resource));
    if (roleResources.isEmpty())
      return null;
    else if (roleResources.size() == roleBinding.resources().size())
      return roleBinding;
    else
      return new RoleBinding(roleBinding.principal(), roleBinding.role(), roleBinding.scope(), roleResources);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RoleBindingFilter)) {
      return false;
    }

    RoleBindingFilter that = (RoleBindingFilter) o;
    return Objects.equals(principal, that.principal) &&
        Objects.equals(role, that.role) &&
        Objects.equals(scope, that.scope) &&
        Objects.equals(resourceFilter, that.resourceFilter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(principal, role, scope, resourceFilter);
  }

  @Override
  public String toString() {
    return "RoleBindingFilter(" +
        "principal=" + principal +
        ", role='" + role + '\'' +
        ", scope='" + scope + '\'' +
        ", resourceFilter=" + resourceFilter +
        ')';
  }
}
