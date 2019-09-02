// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.utils.SecurityUtils;

/**
 * Represents a resource pattern that can be used to define an {@link AccessRule}. This uses
 * resource types that are not pre-defined, enabling this class to be used for defining
 * rules in different components.
 */
public class ResourcePattern implements Comparable<ResourcePattern> {

  public static final ResourcePattern ALL =
      new ResourcePattern(ResourceType.ALL, org.apache.kafka.common.resource.ResourcePattern.WILDCARD_RESOURCE, PatternType.LITERAL);

  private final String name;
  private final ResourceType resourceType;
  private final PatternType patternType;

  public ResourcePattern(String type, String name, PatternType patternType) {
    this(new ResourceType(type), name, patternType);
  }

  public ResourcePattern(@JsonProperty("resourceType") ResourceType resourceType,
                         @JsonProperty("name") String name,
                         @JsonProperty("patternType") PatternType patternType) {
    this.name = Objects.requireNonNull(name, "name");
    this.resourceType = Objects.requireNonNull(resourceType, "resourceType");
    this.patternType = patternType == null ? PatternType.LITERAL : patternType;
  }

  @JsonProperty
  public String name() {
    return name;
  }

  @JsonProperty
  public ResourceType resourceType() {
    return resourceType;
  }

  @JsonProperty
  public PatternType patternType() {
    return patternType;
  }

  public ResourcePatternFilter toFilter() {
    return new ResourcePatternFilter(resourceType, name, patternType);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ResourcePattern)) {
      return false;
    }

    ResourcePattern that = (ResourcePattern) o;
    return Objects.equals(this.name, that.name) &&
        Objects.equals(this.resourceType, that.resourceType) &&
        Objects.equals(this.patternType, that.patternType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, resourceType, patternType);
  }

  @Override
  public int compareTo(ResourcePattern other) {
    int result = resourceType().name().compareTo(other.resourceType().name());
    if (result == 0) {
      result = patternType().compareTo(other.patternType());
      if (result == 0) {
        result = other.name().compareTo(name()); // reverse ordering for name
      }
    }
    return result;
  }

  @Override
  public String toString() {
    return String.format("%s:%s:%s", resourceType, patternType, name); // Same format as AK
  }

  public static ResourcePattern all(ResourceType resourceType) {
    return new ResourcePattern(resourceType, org.apache.kafka.common.resource.ResourcePattern.WILDCARD_RESOURCE, PatternType.LITERAL);
  }

  public static ResourcePattern from(org.apache.kafka.common.resource.ResourcePattern resourcePattern) {
    return new ResourcePattern(
        SecurityUtils.toPascalCase(resourcePattern.resourceType().name()),
        resourcePattern.name(),
        resourcePattern.patternType());
  }

  public static org.apache.kafka.common.resource.ResourcePattern to(ResourcePattern resourcePattern) {
    return new org.apache.kafka.common.resource.ResourcePattern(
        SecurityUtils.resourceType(resourcePattern.resourceType.name()),
        resourcePattern.name(),
        resourcePattern.patternType());
  }

  public static ResourcePattern from(org.apache.kafka.common.resource.ResourcePatternFilter resourcePattern) {
    return new ResourcePattern(
        SecurityUtils.toPascalCase(resourcePattern.resourceType().name()),
        resourcePattern.name(),
        resourcePattern.patternType());
  }
}
