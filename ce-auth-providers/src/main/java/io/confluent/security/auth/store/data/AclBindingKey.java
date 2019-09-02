// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;

import java.util.Objects;

public class AclBindingKey extends AuthKey {

  private final ResourcePattern resourcePattern;
  private final Scope scope;

  @JsonCreator
  public AclBindingKey(@JsonProperty("resourcePattern") ResourcePattern resourcePattern,
                       @JsonProperty("scope") Scope scope) {
    this.resourcePattern = resourcePattern;
    this.scope = scope;
  }

  @JsonProperty
  public ResourcePattern resourcePattern() {
    return resourcePattern;
  }

  @JsonProperty
  public Scope scope() {
    return scope;
  }

  @JsonIgnore
  @Override
  public AuthEntryType entryType() {
    return AuthEntryType.ACL_BINDING;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    final AclBindingKey that = (AclBindingKey) o;
    return Objects.equals(resourcePattern, that.resourcePattern) &&
        Objects.equals(scope, that.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), resourcePattern, scope);
  }

  @Override
  public String toString() {
    return "AclBindingKey{" +
        "resourcePattern=" + resourcePattern +
        ", scope=" + scope +
        '}';
  }
}
