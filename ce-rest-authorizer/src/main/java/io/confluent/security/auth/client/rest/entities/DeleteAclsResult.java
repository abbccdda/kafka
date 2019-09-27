/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.security.auth.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.confluent.security.auth.client.rest.jackson.AclBindingFilterMapKeyDeserializer;
import io.confluent.security.auth.client.rest.jackson.AclBindingFilterMapKeySerializer;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

public class DeleteAclsResult {

  @JsonSerialize(keyUsing = AclBindingFilterMapKeySerializer.class)
  @JsonDeserialize(keyUsing = AclBindingFilterMapKeyDeserializer.class)
  public final Map<AclBindingFilter, DeleteResult> resultMap;

  @JsonCreator
  public DeleteAclsResult(
      @JsonProperty("resultMap") Map<AclBindingFilter, DeleteResult> resultMap) {
    this.resultMap = resultMap;
  }

  public static class DeleteResult {
    public Boolean success;
    public Collection<AclBinding> aclBindings;
    public String errorMessage;

    @JsonCreator
    DeleteResult(@JsonProperty("success") Boolean success,
                 @JsonProperty("aclBindings") Collection<AclBinding> aclBindings,
                 @JsonProperty("errorMessage") String errorMessage) {
      this.success = success;
      this.aclBindings = aclBindings;
      this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
      return "DeleteResult{" +
          "success=" + success +
          ", aclBindings=" + aclBindings +
          ", errorMessage='" + errorMessage + '\'' +
          '}';
    }
  }

  public static DeleteResult success(Collection<AclBinding> deletedBindings) {
    return new DeleteResult(true, deletedBindings, "");
  }

  public static DeleteResult failure(String errorMessage) {
    return new DeleteResult(false, Collections.emptyList(), errorMessage);
  }

  @Override
  public String toString() {
    return "DeleteAclsResult{" +
        "resultMap=" + resultMap +
        '}';
  }
}
