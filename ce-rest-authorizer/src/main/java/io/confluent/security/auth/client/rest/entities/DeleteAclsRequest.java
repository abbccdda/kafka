/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.security.auth.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.Scope;
import org.apache.kafka.common.acl.AclBindingFilter;

import java.util.Collection;

public class DeleteAclsRequest {

  public final Scope scope;
  public final Collection<AclBindingFilter> aclBindingFilters;

  @JsonCreator
  public DeleteAclsRequest(
      @JsonProperty("scope") Scope scope,
      @JsonProperty("aclBindingFilters") Collection<AclBindingFilter> aclBindingFilters) {
    this.scope = scope;
    this.aclBindingFilters = aclBindingFilters;
  }

  @Override
  public String toString() {
    return "DeleteAclsRequest{"
        + "scope=" + scope
        + ", aclBindingFilters=" + aclBindingFilters
        + '}';
  }
}
