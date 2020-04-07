// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer.acl;

import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.AclAccessRule;
import io.confluent.security.authorizer.AuthorizePolicy.PolicyType;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourceType;
import kafka.security.authorizer.AclEntry;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.utils.SecurityUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Maps Kafka ACL and related classes to Confluent cross-component authorization classes.
 */
public class AclMapper {

  private static final Map<org.apache.kafka.common.resource.ResourceType, ResourceType> RESOURCE_TYPES;
  private static final Map<AclOperation, Operation> OPERATIONS;
  private static final Map<Operation, AclOperation> ACL_OPERATIONS;
  private static final Map<AclPermissionType, io.confluent.security.authorizer.PermissionType> PERMISSION_TYPES;

  static {
    RESOURCE_TYPES = new HashMap<>();
    Stream.of(org.apache.kafka.common.resource.ResourceType.values()).forEach(kafkaResourceType -> {
      ResourceType resourceType = new ResourceType(
          SecurityUtils.toPascalCase(kafkaResourceType.name()));
      RESOURCE_TYPES.put(kafkaResourceType, resourceType);
    });

    OPERATIONS = new HashMap<>();
    ACL_OPERATIONS = new HashMap<>();
    Stream.of(AclOperation.values()).forEach(kafkaOperation -> {
      Operation operation = new Operation(SecurityUtils.toPascalCase(kafkaOperation.name()));
      OPERATIONS.put(kafkaOperation, operation);
      ACL_OPERATIONS.put(operation, kafkaOperation);
    });

    PERMISSION_TYPES = new HashMap<>();
    for (io.confluent.security.authorizer.PermissionType permissionType : io.confluent.security.authorizer.PermissionType
        .values()) {
      AclPermissionType kafkaPermissionType = AclPermissionType.fromString(permissionType.name());
      PERMISSION_TYPES.put(kafkaPermissionType, permissionType);
    }
  }

  public static ResourceType resourceType(org.apache.kafka.common.resource.ResourceType resourceType) {
    return mapValueOrFail(RESOURCE_TYPES, resourceType);
  }

  public static Operation operation(AclOperation operation) {
    return mapValueOrFail(OPERATIONS, operation);
  }

  public static AclOperation aclOperation(Operation operation) {
    return mapValueOrFail(ACL_OPERATIONS, operation);
  }

  public static io.confluent.security.authorizer.PermissionType permissionType(AclPermissionType permissionType) {
    return mapValueOrFail(PERMISSION_TYPES, permissionType);
  }

  private static <K, V> V mapValueOrFail(Map<K, V> map, K key) {
    V value = map.get(key);
    if (value == null)
      throw new IllegalArgumentException(String.format("Value is null for %s", key));
    else
      return value;
  }

  public static AccessRule accessRule(AclEntry aclEntry) {
    AccessControlEntry ace = aclEntry.ace();
    return new AclAccessRule(ResourcePattern.from(aclEntry.aclBinding().pattern()),
        aclEntry.kafkaPrincipal(),
        permissionType(ace.permissionType()),
        ace.host(),
        operation(ace.operation()),
        ace.permissionType() == AclPermissionType.ALLOW ? PolicyType.ALLOW_ACL : PolicyType.DENY_ACL,
        aclEntry.aclBinding());
  }
}
