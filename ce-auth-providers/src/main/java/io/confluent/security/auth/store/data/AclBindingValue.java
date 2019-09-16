// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.acl.AclRule;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class AclBindingValue extends AuthValue {

  private final Set<AclRule> aclRules;

  @JsonCreator
  public AclBindingValue(@JsonProperty("aclRules") Collection<AclRule> aclRules) {
    this.aclRules = aclRules == null ? Collections.emptySet() : new HashSet<>(aclRules);
  }

  @JsonProperty
  public Collection<AclRule> aclRules() {
    return aclRules;
  }

  @JsonIgnore
  @Override
  public AuthEntryType entryType() {
    return AuthEntryType.ACL_BINDING;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AclBindingValue)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    AclBindingValue that = (AclBindingValue) o;

    return Objects.equals(aclRules, that.aclRules);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + Objects.hash(aclRules);
  }

  @Override
  public String toString() {
    return "AclBindingValue{" +
        "accessRules=" + aclRules +
        '}';
  }
}
