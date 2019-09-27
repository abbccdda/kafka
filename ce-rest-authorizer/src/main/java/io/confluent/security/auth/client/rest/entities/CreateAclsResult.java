/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.security.auth.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.confluent.security.auth.client.rest.jackson.AclBindingMapKeyDeserializer;
import io.confluent.security.auth.client.rest.jackson.AclBindingMapKeySerializer;
import org.apache.kafka.common.acl.AclBinding;

import java.util.Map;

public class CreateAclsResult {

  public static final CreateResult SUCCESS = new CreateResult(true, "");

  @JsonSerialize(keyUsing = AclBindingMapKeySerializer.class)
  @JsonDeserialize(keyUsing = AclBindingMapKeyDeserializer.class)
  public final Map<AclBinding, CreateResult> resultMap;

  @JsonCreator
  public CreateAclsResult(
      @JsonProperty("resultMap") Map<AclBinding, CreateResult> resultMap) {
    this.resultMap = resultMap;
  }

  public static class CreateResult {
    public Boolean success;
    public String errorMessage;

    @JsonCreator
    CreateResult(@JsonProperty("success") Boolean success,
                 @JsonProperty("errorMessage") String errorMessage) {
      this.success = success;
      this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
      return "CreateResult{" +
          "success=" + success +
          ", errorMessage='" + errorMessage + '\'' +
          '}';
    }
  }

  public static CreateResult failure(String errorMessage) {
    return new CreateResult(false, errorMessage);
  }

  @Override
  public String toString() {
    return "CreateAclsResult{" +
        "resultMap=" + resultMap +
        '}';
  }
}
