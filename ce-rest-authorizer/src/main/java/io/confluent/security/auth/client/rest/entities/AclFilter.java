/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.security.auth.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.Scope;
import org.apache.kafka.common.acl.AclBindingFilter;

public class AclFilter {

  public final Scope scope;
  public final AclBindingFilter aclBindingFilter;

  @JsonCreator
  public AclFilter(
      @JsonProperty("scope") Scope scope,
      @JsonProperty("aclBindingFilter") AclBindingFilter aclBindingFilter) {
    this.scope = scope;
    this.aclBindingFilter = aclBindingFilter;
  }

  @Override
  public String toString() {
    return "AclFilter{"
        + "scope=" + scope
        + ", aclBindingFilter=" + aclBindingFilter
        + '}';
  }
}
