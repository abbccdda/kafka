// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.data;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME,
              include = JsonTypeInfo.As.PROPERTY,
              property = "_type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(value = RoleBindingKey.class, name = "RoleBinding"),
    @JsonSubTypes.Type(value = UserKey.class, name = "User"),
    @JsonSubTypes.Type(value = StatusKey.class, name = "Status"),
    @JsonSubTypes.Type(value = AclBindingKey.class, name = "AclBinding")
})
public abstract class AuthKey {

  public abstract AuthEntryType entryType();

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AuthKey)) {
      return false;
    }

    AuthKey that = (AuthKey) o;

    return Objects.equals(this.entryType(), that.entryType());
  }

  @Override
  public int hashCode() {
    return Objects.hash(entryType());
  }
}
