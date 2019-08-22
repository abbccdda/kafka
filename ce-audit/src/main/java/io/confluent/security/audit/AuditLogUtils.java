/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.security.audit;

import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.Scope;

public class AuditLogUtils {

  private static AuthorizationInfo.Builder authorizationInfo(Action action, boolean granted) {
    return AuthorizationInfo.newBuilder()
        .setResourceType(action.resourceType().name())
        .setResourceName(action.resourceName())
        .setOperation(action.operation().name())
        .setPatternType(action.resourcePattern().patternType().toString())
        .setGranted(granted);

  }

  /**
   * Return an AuthorizationInfo that captures this ACL authorization
   */
  public static AuthorizationInfo aclAuthorizationInfo(String host, String permissionType,
      Action action, boolean granted) {
    return authorizationInfo(action, granted)
        .setAclAuthorization(AclAuthorizationInfo.newBuilder()
            .setHost(host)
            .setPermissionType(permissionType)
            .build())
        .build();
  }

  private static AuthorizationScope authorizationScope(Scope scope) {
    return AuthorizationScope.newBuilder()
        .addAllOuterScope(scope.path())
        .putAllClusters(scope.clusters())
        .build();
  }

  /**
   * Return an AuthorizationInfo that captures this RBAC authorization
   */
  public static AuthorizationInfo rbacAuthorizationInfo(String role, Scope scope, Action action,
      boolean granted) {
    return authorizationInfo(action, granted)
        .setRbacAuthorization(RbacAuthorizationInfo.newBuilder()
            .setRole(role)
            .setScope(authorizationScope(scope))
            .build())
        .build();
  }

}
