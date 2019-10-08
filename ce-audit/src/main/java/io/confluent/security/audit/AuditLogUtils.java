/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.security.audit;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizePolicy.AccessRulePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.rbac.RoleBinding;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.protocol.ApiKeys;

public class AuditLogUtils {

  public static AuditLogEntry authorizationEvent(String source, String subject,
      RequestContext requestContext, Action action, AuthorizeResult authorizeResult,
      AuthorizePolicy authorizePolicy) {

    String requestName;
    int requestType = requestContext.requestType();
    if (requestType < 0) {
      if (requestContext.MDS.equals(requestContext.requestSource())) {
        requestName = "Authorize";
      } else {
        throw new RuntimeException("Got unexpected requestType not from MDS: " + requestType);
      }
    } else {
      ApiKeys requestKey = ApiKeys.forId(requestType);
      if (requestKey == ApiKeys.FETCH) {
        if (AclOperation.CLUSTER_ACTION.equals(action.operation())) {
          requestName = "FetchFollower";
        } else {
          requestName = "FetchConsumer";
        }
      } else {
        requestName = requestKey.name;
      }
    }


    AuditLogEntry.Builder builder = AuditLogEntry.newBuilder()
        .setServiceName(source)
        .setMethodName(requestContext.requestSource() + "." + requestName)
        .setResourceName(subject);

    AuthenticationInfo.Builder authenticationBuilder = AuthenticationInfo.newBuilder()
        .setPrincipal(requestContext.principal().toString());
    builder.setAuthenticationInfo(authenticationBuilder);

    AuthorizationInfo.Builder authorizationBuilder = AuthorizationInfo.newBuilder()
        .setGranted(authorizeResult == AuthorizeResult.ALLOWED)
        .setOperation(action.operation().name())
        .setResourceType(action.resourcePattern().resourceType().toString())
        .setResourceName(action.resourcePattern().name())
        .setPatternType(action.resourcePattern().patternType().toString());
    builder.setAuthorizationInfo(authorizationBuilder);

    switch (authorizePolicy.policyType()) {
      case SUPER_USER:
      case SUPER_GROUP:
        authorizationBuilder
            .setSuperUserAuthorization(true);
        break;
      case ALLOW_ACL:
      case DENY_ACL:
        AccessControlEntry entry =
            ((AclBinding) ((AccessRulePolicy) authorizePolicy).sourceMetadata()).entry();
        authorizationBuilder
            .setAclAuthorization(AclAuthorizationInfo.newBuilder()
                .setHost(entry.host())
                .setPermissionType(entry.permissionType().toString()));
        break;
      case ALLOW_ROLE:
        RoleBinding roleBinding =
            (RoleBinding) ((AccessRulePolicy) authorizePolicy).sourceMetadata();
        authorizationBuilder
            .setRbacAuthorization(RbacAuthorizationInfo.newBuilder()
                .setRole(roleBinding.role())
                .setScope(AuthorizationScope.newBuilder()
                    .addAllOuterScope(roleBinding.scope().path())
                    .putAllClusters(roleBinding.scope().clusters())));
        break;
      case NO_MATCHING_RULE:
      case DENY_ON_NO_RULE:
      case ALLOW_ON_NO_RULE:
        break;
    }

    Struct.Builder requestBuilder = Struct.newBuilder()
        .putFields("correlation_id", Value.newBuilder()
            .setStringValue(String.valueOf(requestContext.correlationId())).build());
    if (requestContext.clientId() != null) {
      requestBuilder.putFields("client_id",
          Value.newBuilder().setStringValue(requestContext.clientId()).build());
    }
    builder.setRequest(requestBuilder.build());

    Struct.Builder requestMetadataBuilder = Struct.newBuilder();
    if (requestContext.clientAddress() != null) {
      requestMetadataBuilder.putFields("client_address", Value.newBuilder()
          .setStringValue(requestContext.clientAddress().toString()).build());
    }
    builder.setRequestMetadata(requestMetadataBuilder.build());

    return builder.build();
  }

}
