// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;

/**
 * Encapsulates an access rule which may be derived from an ACL or RBAC policy.
 * Operations and resource types are extensible to enable this to be used for
 * authorization in different components.
 */
public class AccessRule {

  public static final String ALL_HOSTS = "*";
  public static final KafkaPrincipal WILDCARD_USER_PRINCIPAL = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "*");
  public static final String GROUP_PRINCIPAL_TYPE = "Group";
  public static final KafkaPrincipal WILDCARD_GROUP_PRINCIPAL = new KafkaPrincipal(GROUP_PRINCIPAL_TYPE, "*");

  private final KafkaPrincipal principal;
  private final PermissionType permissionType;
  @JsonInclude(JsonInclude.Include.ALWAYS)
  private final String host;
  private final Operation operation;
  private final String sourceDescription;

  @JsonCreator
  public AccessRule(@JsonProperty("principal") KafkaPrincipal principal,
                    @JsonProperty("permissionType") PermissionType permissionType,
                    @JsonProperty("host") String host,
                    @JsonProperty("operation") Operation operation,
                    @JsonProperty("sourceDescription") String sourceDescription) {
    this.principal = principal;
    this.permissionType = permissionType;
    this.operation = operation;
    this.host = host;
    this.sourceDescription = sourceDescription;
  }

  @JsonProperty
  public KafkaPrincipal principal() {
    return principal;
  }

  @JsonProperty
  public PermissionType permissionType() {
    return permissionType;
  }

  @JsonProperty
  public String host() {
    return host;
  }

  @JsonProperty
  public Operation operation() {
    return operation;
  }

  @JsonProperty
  public String sourceDescription() {
    return sourceDescription;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AccessRule)) {
      return false;
    }

    AccessRule that = (AccessRule) o;
    return Objects.equals(this.principal, that.principal) &&
        Objects.equals(this.permissionType, that.permissionType) &&
        Objects.equals(this.host, that.host) &&
        Objects.equals(this.operation, that.operation) &&
        Objects.equals(this.sourceDescription, that.sourceDescription);
  }

  @Override
  public int hashCode() {
    return Objects.hash(principal, permissionType, host, operation, sourceDescription);
  }

  @Override
  public String toString() {
    return String.format("%s has %s permission for operation %s from host %s (source: %s)",
        principal, permissionType, operation, host, sourceDescription);
  }

  public static AccessRule from(final AccessControlEntry entry) {
    return new AccessRule(SecurityUtils.parseKafkaPrincipal(entry.principal()),
        PermissionType.valueOf(entry.permissionType().name()),
        entry.host(),
        new Operation(SecurityUtils.toPascalCase(entry.operation().name())),
        entry.toString());
  }

  public static AccessControlEntry to(final  AccessRule rule) {
    return new AccessControlEntry(rule.principal.toString(),
        rule.host,
        SecurityUtils.operation(rule.operation.name()),
        AclPermissionType.fromString(rule.permissionType.name()));
  }

  public static AccessRule from(final AccessControlEntryFilter filter) {
    return new AccessRule(SecurityUtils.parseKafkaPrincipal(filter.principal()),
        PermissionType.valueOf(filter.permissionType().name()),
        filter.host(),
        new Operation(SecurityUtils.toPascalCase(filter.operation().name())),
        filter.toString());
  }
}
