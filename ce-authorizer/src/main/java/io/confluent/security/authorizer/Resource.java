// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.apache.kafka.common.resource.PatternType;

/**
 * Represents an authorizable resource with resource types that are not pre-defined, enabling this
 * class to be used for authorization in different components.
 */
public class Resource {

  private final String name;
  private final ResourceType resourceType;

  public Resource(String type, String name) {
    this(new ResourceType(type), name);
  }

  public Resource(@JsonProperty("resourceType") ResourceType resourceType,
                  @JsonProperty("name") String name) {
    this.name = name;
    this.resourceType = resourceType;
  }

  @JsonProperty
  public String name() {
    return name;
  }

  @JsonProperty
  public ResourceType resourceType() {
    return resourceType;
  }

  public ResourcePattern toResourcePattern() {
    return new ResourcePattern(resourceType, name, PatternType.LITERAL);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Resource)) {
      return false;
    }

    Resource that = (Resource) o;
    return Objects.equals(this.name, that.name) &&
        Objects.equals(this.resourceType, that.resourceType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, resourceType);
  }

  @Override
  public String toString() {
    return String.format("%s:%s", resourceType, name);
  }
}
