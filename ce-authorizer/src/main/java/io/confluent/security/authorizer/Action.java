// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.resource.PatternType;

/**
 * Represents an authorizable action, which is an operation performed on a resource.
 */
public class Action {

  private final Scope scope;
  private final ResourcePattern resourcePattern;
  private final Operation operation;
  private final int resourceReferenceCount;
  private final boolean logIfAllowed;
  private final boolean logIfDenied;

  /**
   * Constructs a cross-component authorizable action for a literal resource.
   * @param scope Scope of resource
   * @param resourceType Resource type
   * @param resourceName Non-null name of resource
   * @param operation Operation being performed on resource
   */
  public Action(@JsonProperty("scope") Scope scope,
                @JsonProperty("resourceType") ResourceType resourceType,
                @JsonProperty("resourceName") String resourceName,
                @JsonProperty("operation") Operation operation) {
    this(scope, new ResourcePattern(resourceType, resourceName, PatternType.LITERAL), operation);
  }


  /**
   * Constructs an authorizable action
   * @param scope Scope of resource
   * @param resourcePattern Resource pattern must be literal for Kafka and other components
   *    using Metadata Service for authorization. Metadata Service may authorize actions with
   *    any pattern type including ANY and MATCH.
   * @param operation Operation being performed on resource
   */
  public Action(Scope scope, ResourcePattern resourcePattern, Operation operation) {
    this(scope, resourcePattern, operation, 1, true, true);
  }

  /**
   * Constructs an authorizable action
   * @param scope Scope of resource
   * @param resourcePattern Resource pattern must be literal for Kafka and other components
   *    using Metadata Service for authorization. Metadata Service may authorize actions with
   *    any pattern type including ANY and MATCH.
   * @param operation Operation being performed on resource
   * @param resourceReferenceCount Number of times the resource is referenced in the request
   * @param logIfAllowed Enable audit logging if permission is granted
   * @param logIfDenied Enable audit logging if permission is denied
   */
  public Action(Scope scope,
                ResourcePattern resourcePattern,
                Operation operation,
                int resourceReferenceCount,
                boolean logIfAllowed,
                boolean logIfDenied) {
    this.scope = Objects.requireNonNull(scope, "scope");
    this.resourcePattern = resourcePattern;
    this.operation = operation == null ? Operation.ALL : operation;
    this.resourceReferenceCount = resourceReferenceCount;
    this.logIfAllowed = logIfAllowed;
    this.logIfDenied = logIfDenied;
  }

  @JsonProperty
  public Scope scope() {
    return scope;
  }

  @JsonProperty
  public ResourceType resourceType() {
    return resourcePattern.resourceType();
  }

  @JsonProperty
  public String resourceName() {
    return resourcePattern.name();
  }

  @JsonProperty
  public Operation operation() {
    return operation;
  }

  public ResourcePattern resourcePattern() {
    return resourcePattern;
  }

  public int resourceReferenceCount() {
    return resourceReferenceCount;
  }

  public boolean logIfAllowed() {
    return logIfAllowed;
  }

  public boolean logIfDenied() {
    return logIfDenied;
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
            Objects.equals(this.resourcePattern, that.resourcePattern) &&
            Objects.equals(this.operation, that.operation) &&
            resourceReferenceCount == that.resourceReferenceCount &&
            logIfAllowed == that.logIfAllowed &&
            logIfDenied == that.logIfDenied;

  }

  @Override
  public int hashCode() {
    return Objects.hash(scope, resourcePattern, operation);
  }

  @Override
  public String toString() {
    return "Action(" +
            "scope='" + scope + '\'' +
            ", resourcePattern='" + resourcePattern + '\'' +
            ", operation='" + operation + '\'' +
            ", resourceReferenceCount='" + resourceReferenceCount + '\'' +
            ", logIfAllowed='" + logIfAllowed + '\'' +
            ", logIfDenied='" + logIfDenied + '\'' +
            ')';
  }
}
