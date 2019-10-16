// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.acl;

import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.PermissionType;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;

/**
 * ACL rule for centralized ACLs.
 */
public class AclRule {

  private final KafkaPrincipal principal;
  private final PermissionType permissionType;
  @JsonInclude(JsonInclude.Include.ALWAYS)
  private final String host;
  private final Operation operation;

  @JsonCreator
  public AclRule(@JsonProperty("principal") KafkaPrincipal principal,
                 @JsonProperty("permissionType") PermissionType permissionType,
                 @JsonProperty("host") String host,
                 @JsonProperty("operation") Operation operation) {
    this.principal = principal;
    this.permissionType = permissionType;
    this.operation = operation;
    this.host = host;
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

  public AccessControlEntry toAccessControlEntry() {
    return new AccessControlEntry(principal.toString(),
        host,
        SecurityUtils.operation(operation.name()),
        AclPermissionType.fromString(permissionType.name()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AclRule)) {
      return false;
    }

    AclRule that = (AclRule) o;
    return Objects.equals(this.principal, that.principal) &&
        Objects.equals(this.permissionType, that.permissionType) &&
        Objects.equals(this.host, that.host) &&
        Objects.equals(this.operation, that.operation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(principal, permissionType, host, operation);
  }

  @Override
  public String toString() {
    return String.format("%s has %s permission for operation %s from host %s",
        principal, permissionType, operation, host);
  }

  public static AclRule from(final AclBinding aclBinding) {
    AccessControlEntry entry = aclBinding.entry();
    return new AclRule(SecurityUtils.parseKafkaPrincipal(entry.principal()),
        PermissionType.valueOf(entry.permissionType().name()),
        entry.host(),
        new Operation(SecurityUtils.toPascalCase(entry.operation().name())));
  }

  public static AclRule from(final AccessRule rule) {
    return new AclRule(rule.principal(),
        rule.permissionType(),
        rule.host(),
        rule.operation());
  }

  public static AclRule from(final AccessControlEntryFilter filter) {
    return new AclRule(SecurityUtils.parseKafkaPrincipal(filter.principal()),
        PermissionType.valueOf(filter.permissionType().name()),
        filter.host(),
        new Operation(SecurityUtils.toPascalCase(filter.operation().name())));
  }

  public static AccessControlEntry accessControlEntry(final AccessRule rule) {
    return new AccessControlEntry(rule.principal().toString(),
        rule.host(),
        SecurityUtils.operation(rule.operation().name()),
        AclPermissionType.fromString(rule.permissionType().name()));
  }
}
