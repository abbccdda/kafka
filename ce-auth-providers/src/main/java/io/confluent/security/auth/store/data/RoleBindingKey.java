// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.Scope;
import java.util.Objects;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class RoleBindingKey extends AuthKey {

  private final KafkaPrincipal principal;
  private final String role;
  private final Scope scope;

  @JsonCreator
  public RoleBindingKey(@JsonProperty("principal") KafkaPrincipal principal,
                        @JsonProperty("role") String role,
                        @JsonProperty("scope") Scope scope) {
    this.principal = principal;
    this.role = role;
    this.scope = scope;
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

  @JsonIgnore
  @Override
  public AuthEntryType entryType() {
    return AuthEntryType.ROLE_BINDING;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RoleBindingKey)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    RoleBindingKey that = (RoleBindingKey) o;
    return Objects.equals(principal, that.principal) &&
        Objects.equals(role, that.role) &&
        Objects.equals(scope, that.scope);
  }

  @Override
  public int hashCode() {
    return Objects.hash(principal, role, scope);
  }

  @Override
  public String toString() {
    return "RoleBindingKey{" +
        "principal=" + principal +
        ", role='" + role + '\'' +
        ", scope='" + scope + '\'' +
        '}';
  }
}
