/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.crn;

import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import java.util.Objects;

/**
 * When dealing with resources in more than one scope, it is useful to be able to pass the Scope and
 * ResourcePattern as a single object.
 */
public class ScopedResourcePattern {

  private final Scope scope;
  private final ResourcePattern resourcePattern;

  public ScopedResourcePattern(Scope scope, ResourcePattern resourcePattern) {
    this.scope = scope;
    this.resourcePattern = resourcePattern;
  }

  public Scope scope() {
    return scope;
  }

  public ResourcePattern resourcePattern() {
    return resourcePattern;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof ScopedResourcePattern)) {
      return false;
    }
    ScopedResourcePattern other = (ScopedResourcePattern) obj;
    return scope.equals(other.scope()) && (
        resourcePattern == null && other.resourcePattern() == null ||
        resourcePattern.equals(((ScopedResourcePattern) obj).resourcePattern()));
  }

  @Override
  public int hashCode() {
    return Objects.hash(scope, resourcePattern);
  }

  @Override
  public String toString() {
    return "ScopedResourcePattern(" +
        "scope=" + scope +
        ", resource pattern=" + resourcePattern +
        ")";
  }
}
