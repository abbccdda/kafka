/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.security.auth.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.Scope;
import org.apache.kafka.common.acl.AclBinding;

import java.util.Collection;

public class CreateAclsRequest {

  public final Scope scope;
  public final Collection<AclBinding> aclBindings;

  @JsonCreator
  public CreateAclsRequest(
      @JsonProperty("scope") Scope scope,
      @JsonProperty("aclBindings") Collection<AclBinding> aclBindings) {
    this.scope = scope;
    this.aclBindings = aclBindings;
  }

  @Override
  public String toString() {
    return "CreateAclsRequest{"
        + "scope=" + scope
        + ", aclBindings=" + aclBindings
        + '}';
  }
}
