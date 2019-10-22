/*
 * Copyright 2019 Confluent Inc.
 */

package io.confluent.security.authorizer.jackson;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

public class KafkaModule extends SimpleModule {

  public KafkaModule() {
    super("KafkaModule");

    setMixInAnnotation(AclBinding.class, AclBindingMixin.class);
    setMixInAnnotation(ResourcePattern.class, ResourcePatternMixin.class);
    setMixInAnnotation(AccessControlEntry.class, AccessControlEntryMixin.class);
    setMixInAnnotation(AclBindingFilter.class, AclBindingFilterMixin.class);
    setMixInAnnotation(ResourcePatternFilter.class, ResourcePatternFilterMixin.class);
    setMixInAnnotation(AccessControlEntryFilter.class, AccessControlEntryFilterMixin.class);
  }

  @JsonStrict
  abstract static class AclBindingMixin {

    @JsonCreator
    AclBindingMixin(
        @JsonProperty("pattern") ResourcePattern resourcePattern,
        @JsonProperty("entry") AccessControlEntry entry
    ) {}

    @JsonProperty("pattern")
    abstract ResourcePattern pattern();

    @JsonProperty("entry")
    abstract AccessControlEntry entry();
  }

  @JsonStrict
  abstract static class ResourcePatternMixin {

    @JsonCreator
    public ResourcePatternMixin(
        @JsonProperty("resourceType") ResourceType resourceType,
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty("name") String name,
        @JsonProperty("patternType") PatternType patternType
    ) {}

    @JsonProperty("resourceType")
    abstract ResourceType resourceType();

    @JsonProperty("name")
    abstract String name();

    @JsonProperty("patternType")
    abstract PatternType patternType();
  }

  @JsonStrict
  abstract static class AccessControlEntryMixin {

    @JsonCreator
    AccessControlEntryMixin(
        @JsonProperty("principal") String principal,
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonProperty("host") String host,
        @JsonProperty("operation") AclOperation operation,
        @JsonProperty("permissionType") AclPermissionType permissionType
    ) {}

    @JsonProperty("principal")
    abstract String principal();

    @JsonProperty("host")
    abstract String host();

    @JsonProperty("operation")
    abstract AclOperation operation();

    @JsonProperty("permissionType")
    abstract AclPermissionType permissionType();
  }

  @JsonStrict
  abstract static class AclBindingFilterMixin {

    @JsonCreator
    public AclBindingFilterMixin(
        @JsonProperty("patternFilter") ResourcePatternFilter patternFilter,
        @JsonProperty("entryFilter") AccessControlEntryFilter entryFilter
    ) {}

    @JsonProperty("patternFilter")
    abstract ResourcePatternFilter patternFilter();

    @JsonProperty("entryFilter")
    abstract AccessControlEntryFilter entryFilter();
  }

  @JsonStrict
  abstract static class ResourcePatternFilterMixin {

    @JsonCreator
    ResourcePatternFilterMixin(
        @JsonProperty("resourceType") ResourceType resourceType,
        @JsonProperty("name") String name,
        @JsonProperty("patternType") PatternType patternType
    ) {}

    @JsonProperty("resourceType")
    abstract ResourceType resourceType();

    @JsonProperty("name")
    abstract String name();

    @JsonProperty("patternType")
    abstract PatternType patternType();
  }

  @JsonStrict
  abstract static class AccessControlEntryFilterMixin {

    @JsonCreator
    AccessControlEntryFilterMixin(
        @JsonProperty("principal") String principal,
        @JsonProperty("host") String host,
        @JsonProperty("operation") AclOperation operation,
        @JsonProperty("permissionType") AclPermissionType permissionType
    ) {}

    @JsonProperty("principal")
    abstract String principal();

    @JsonProperty("host")
    abstract String host();

    @JsonProperty("operation")
    abstract AclOperation operation();

    @JsonProperty("permissionType")
    abstract AclPermissionType permissionType();
  }

}
