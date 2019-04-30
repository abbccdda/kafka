// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents an authorizable action, which is an operation performed on a resource.
 */
public class Action {

  private final Scope scope;
  private final ResourceType resourceType;
  private final String resourceName;
  private final Operation operation;

  public Action(@JsonProperty("scope") Scope scope,
                @JsonProperty("resourceType") ResourceType resourceType,
                @JsonProperty("resourceName") String resourceName,
                @JsonProperty("operation") Operation operation) {
    this.scope = scope;
    this.resourceType = resourceType;
    this.resourceName = resourceName;
    this.operation = operation;
  }

  @JsonProperty
  public Scope scope() {
    return scope;
  }

  @JsonProperty
  public ResourceType resourceType() {
    return resourceType;
  }

  @JsonProperty
  public String resourceName() {
    return resourceName;
  }

  @JsonProperty
  public Operation operation() {
    return operation;
  }

  public Resource resource() {
    return new Resource(resourceType, resourceName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Action)) {
      return false;
    }

    Action that = (Action) o;
    return Objects.equals(this.scope, that.scope) &&
            Objects.equals(this.resourceType, that.resourceType) &&
            Objects.equals(this.resourceName, that.resourceName) &&
            Objects.equals(this.operation, that.operation);

  }

  @Override
  public int hashCode() {
    return Objects.hash(scope, resourceType, resourceName, operation);
  }

  @Override
  public String toString() {
    return "Action(" +
            "scope='" + scope + '\'' +
            ", resourceType='" + resourceType + '\'' +
            ", resourceName='" + resourceName + '\'' +
            ", operation='" + operation + '\'' +
            ')';
  }
}
