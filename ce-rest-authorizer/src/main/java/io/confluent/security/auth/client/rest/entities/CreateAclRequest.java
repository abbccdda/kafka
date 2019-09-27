/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.security.auth.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.authorizer.Scope;
import org.apache.kafka.common.acl.AclBinding;

public class CreateAclRequest {

  public final Scope scope;
  public final AclBinding aclBinding;

  @JsonCreator
  public CreateAclRequest(
      @JsonProperty("scope") Scope scope,
      @JsonProperty("aclBinding") AclBinding aclBinding) {
    this.scope = scope;
    this.aclBinding = aclBinding;
  }

  @Override
  public String toString() {
    return "CreateAclRequest{"
        + "scope=" + scope
        + ", aclBinding=" + aclBinding
        + '}';
  }
}
