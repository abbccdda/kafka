/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.security.audit;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import io.confluent.crn.ConfluentResourceName;
import io.confluent.crn.ConfluentResourceName.Element;
import io.confluent.crn.ConfluentServerCrnAuthority;
import io.confluent.crn.CrnSyntaxException;
import io.confluent.kafka.security.audit.event.ConfluentAuthenticationEvent;
import io.confluent.security.authorizer.AclAccessRule;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.provider.ConfluentAuthorizationEvent;
import io.confluent.security.rbac.RbacAccessRule;
import io.confluent.security.rbac.RoleBinding;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.protocol.ApiKeys;

import java.util.Map;

public class AuditLogUtils {

  private static void addAuthorizationInfo(AuthorizationInfo.Builder authorizationBuilder,
      AuthorizePolicy authorizePolicy) {
    switch (authorizePolicy.policyType()) {
      case NO_MATCHING_RULE:
      case DENY_ON_NO_RULE:
      case ALLOW_ON_NO_RULE:
        break;
      case SUPER_USER:
      case SUPER_GROUP:
        authorizationBuilder
            .setSuperUserAuthorization(true);
        break;
      case ALLOW_ACL:
      case DENY_ACL:
        AccessControlEntry entry =
            ((AclAccessRule) authorizePolicy).aclBinding().entry();
        authorizationBuilder
            .setAclAuthorization(AclAuthorizationInfo.newBuilder()
                .setHost(entry.host())
                .setPermissionType(entry.permissionType().toString()));
        break;
      case ALLOW_ROLE:
        RoleBinding roleBinding =
            ((RbacAccessRule) authorizePolicy).roleBinding();
        authorizationBuilder
            .setRbacAuthorization(RbacAuthorizationInfo.newBuilder()
                .setRole(roleBinding.role())
                .setScope(AuthorizationScope.newBuilder()
                    .addAllOuterScope(roleBinding.scope().path())
                    .putAllClusters(roleBinding.scope().clusters())));
        break;
    }
  }

  public static AuditLogEntry authorizationEvent(ConfluentAuthorizationEvent authorizationEvent,
                                                 ConfluentServerCrnAuthority crnAuthority) throws CrnSyntaxException {

    String source = crnAuthority.canonicalCrn(authorizationEvent.sourceScope()).toString();
    String subject = crnAuthority.canonicalCrn(authorizationEvent.action().scope(), authorizationEvent.action().resourcePattern())
        .toString();

    String requestName;
    int requestType = authorizationEvent.requestContext().requestType();
    if (requestType < 0) {
      if (RequestContext.MDS.equals(authorizationEvent.requestContext().requestSource())) {
        requestName = "Authorize";
      } else {
        throw new RuntimeException("Got unexpected requestType not from MDS: " + requestType);
      }
    } else {
      ApiKeys requestKey = ApiKeys.forId(requestType);
      if (requestKey == ApiKeys.FETCH) {
        // Ideally, we'd use the mapping in AclMapper, but this doesn't depend on ce-broker-plugins
        if ("ClusterAction".equals(authorizationEvent.action().operation().name())) {
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
        .setMethodName(authorizationEvent.requestContext().requestSource() + "." + requestName)
        .setResourceName(subject);

    String principal = authorizationEvent.requestContext().principal().getPrincipalType() + ":" +
        authorizationEvent.requestContext().principal().getName();
    AuthenticationInfo.Builder authenticationBuilder = AuthenticationInfo.newBuilder()
        .setPrincipal(principal);
    builder.setAuthenticationInfo(authenticationBuilder);

    AuthorizationInfo.Builder authorizationBuilder = AuthorizationInfo.newBuilder()
        .setGranted(authorizationEvent.authorizeResult() == AuthorizeResult.ALLOWED)
        .setOperation(authorizationEvent.action().operation().name())
        .setResourceType(authorizationEvent.action().resourcePattern().resourceType().toString())
        .setResourceName(authorizationEvent.action().resourcePattern().name())
        .setPatternType(authorizationEvent.action().resourcePattern().patternType().toString());

    addAuthorizationInfo(authorizationBuilder, authorizationEvent.authorizePolicy());
    builder.setAuthorizationInfo(authorizationBuilder);

    Struct.Builder requestBuilder = Struct.newBuilder()
        .putFields("correlation_id", Value.newBuilder()
            .setStringValue(String.valueOf(authorizationEvent.requestContext().correlationId())).build());
    if (authorizationEvent.requestContext().clientId() != null) {
      requestBuilder.putFields("client_id",
          Value.newBuilder().setStringValue(authorizationEvent.requestContext().clientId()).build());
    }
    builder.setRequest(requestBuilder.build());

    Struct.Builder requestMetadataBuilder = Struct.newBuilder();
    if (authorizationEvent.requestContext().clientAddress() != null) {
      requestMetadataBuilder.putFields("client_address", Value.newBuilder()
          .setStringValue(authorizationEvent.requestContext().clientAddress().toString()).build());
    }
    builder.setRequestMetadata(requestMetadataBuilder.build());

    return builder.build();
  }

  /**
   * This returns the last element of the resource in the "resourceName" field.
   */
  public static Element resourceNameElement(AuditLogEntry entry) throws CrnSyntaxException {
    return ConfluentResourceName.fromString(entry.getResourceName()).lastResourceElement();
  }

  public static AuditLogEntry authenticationEvent(ConfluentAuthenticationEvent authenticationEvent,
                                                  ConfluentServerCrnAuthority crnAuthority) throws CrnSyntaxException {
    String source = crnAuthority.canonicalCrn(authenticationEvent.getScope()).toString();
    AuditLogEntry.Builder builder = AuditLogEntry.newBuilder()
        .setServiceName(source)
        .setMethodName("kafka.Authentication")
        .setResourceName(source);

    //set authenticationInfo
    AuthenticationInfo.Builder authenticationBuilder = AuthenticationInfo.newBuilder();
    authenticationEvent.principal().ifPresent(p ->
        authenticationBuilder.setPrincipal(p.getPrincipalType() + ":" + p.getName()));

    // set auth metadata
    AuthenticationMetadata.Builder metadataBuilder = authenticationBuilder.getMetadataBuilder();
    Map<String, Object> data = authenticationEvent.data();
    metadataBuilder.setIdentifier((String) data.getOrDefault("identifier", ""));
    metadataBuilder.setMechanism((String) data.getOrDefault("mechanism", ""));
    authenticationBuilder.setMetadata(metadataBuilder.build());

    builder.setAuthenticationInfo(authenticationBuilder);

    //set status
    Result.Builder resultBuilder = Result.newBuilder();
    resultBuilder.setStatus(authenticationEvent.status().name());
    resultBuilder.setMessage((String) data.getOrDefault("message", ""));
    builder.setResult(resultBuilder.build());

    //set request_metadata
    Struct.Builder requestMetadataBuilder = Struct.newBuilder();
    if (authenticationEvent.authenticationContext().clientAddress() != null) {
      requestMetadataBuilder.putFields("client_address", Value.newBuilder()
          .setStringValue(authenticationEvent.authenticationContext().clientAddress().toString()).build());
    }
    builder.setRequestMetadata(requestMetadataBuilder.build());

    return builder.build();
  }

}
