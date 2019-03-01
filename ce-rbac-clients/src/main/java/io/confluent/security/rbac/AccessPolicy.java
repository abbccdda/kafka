// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.rbac;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.security.authorizer.Operation;
import io.confluent.kafka.security.authorizer.ResourceType;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Utils;


/**
 * Defines the access policy corresponding to each role. Roles are currently statically defined
 * in the JSON file `rbac_policy.json`.
 */
public class AccessPolicy {

  public static final String CLUSTER_SCOPE = "Cluster";
  public static final String RESOURCE_SCOPE = "Resource";
  static final Set<String> SCOPES = Utils.mkSet(CLUSTER_SCOPE, RESOURCE_SCOPE);

  private final String scope;
  private final boolean isSuperUser;
  private final Map<ResourceType, Collection<Operation>> allowedOperations;

  @JsonCreator
  public AccessPolicy(@JsonProperty("scope") String scope,
                      @JsonProperty("isSuperUser") boolean isSuperUser,
                      @JsonProperty("allowedOperations") Collection<ResourceOperations> allowedOperations) {
    this.scope = scope;
    this.isSuperUser = isSuperUser;
    if (isSuperUser) {
      this.allowedOperations = Collections.singletonMap(ResourceType.ALL, Collections.singleton(Operation.ALL));
    } else {
      this.allowedOperations = allowedOperations.stream()
          .collect(Collectors.toMap(op -> new ResourceType(op.resourceType),
              op -> op.operations.stream().map(Operation::new).collect(Collectors.toList())));
    }
  }

  @JsonProperty
  public String scope() {
    return scope;
  }

  @JsonProperty
  public boolean isSuperUser() {
    return isSuperUser;
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
    return AccessPolicy.RESOURCE_SCOPE.equalsIgnoreCase(scope);
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

    return Objects.equals(this.scope, that.scope) &&
        Objects.equals(this.isSuperUser, that.isSuperUser) &&
        Objects.equals(this.allowedOperations, that.allowedOperations);
  }

  @Override
  public int hashCode() {
    return Objects.hash(scope, isSuperUser, allowedOperations);
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
