// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.ScopeType;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;


/**
 * Defines the access policy corresponding to each role. Roles are currently statically defined
 * in the JSON file `rbac_policy.json`.
 */
public class AccessPolicy {

  private final ScopeType scopeType;
  private final Map<ResourceType, Collection<Operation>> allowedOperations;

  @JsonCreator
  public AccessPolicy(@JsonProperty("scopeType") String scopeType,
                      @JsonProperty("allowedOperations") Collection<ResourceOperations> allowedOperations) {
    ScopeType type;
    try {
      type = ScopeType.valueOf(scopeType.toUpperCase(Locale.ROOT));
    } catch (NullPointerException npe) {
      type = ScopeType.UNKNOWN;
    }
    this.scopeType = type;
    this.allowedOperations = allowedOperations.stream()
        .collect(Collectors.toMap(op -> new ResourceType(op.resourceType),
            op -> op.operations.stream().map(Operation::new).collect(Collectors.toList())));
  }

  @JsonProperty
  public ScopeType scopeType() {
    return scopeType;
  }

  @JsonProperty
  public Collection<ResourceOperations> allowedOperations() {
    return allowedOperations.entrySet().stream()
        .map(e -> new ResourceOperations(e.getKey().name(),
            e.getValue().stream().map(Operation::name).collect(Collectors.toSet())))
        .collect(Collectors.toSet());
  }

  public Collection<Operation> allowedOperations(ResourceType resourceType) {
    Collection<Operation> ops =  allowedOperations.get(resourceType);
    return ops == null ? Collections.emptySet() : ops;
  }

  @JsonIgnore
  public boolean hasResourceScope() {
    return ScopeType.RESOURCE == scopeType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AccessPolicy)) {
      return false;
    }

    AccessPolicy that = (AccessPolicy) o;

    return Objects.equals(this.scopeType, that.scopeType) &&
        Objects.equals(this.allowedOperations, that.allowedOperations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scopeType, allowedOperations);
  }

  public static class ResourceOperations {
    private final String resourceType;
    private final Collection<String> operations;

    @JsonCreator
    public ResourceOperations(@JsonProperty("resourceType") String resourceType,
                              @JsonProperty("operations") Collection<String> operations) {
      this.resourceType = resourceType;
      this.operations = operations;
    }

    @JsonProperty
    public String resourceType() {
      return resourceType;
    }

    @JsonProperty
    public Collection<String> operations() {
      return operations;
    }
  }
}
