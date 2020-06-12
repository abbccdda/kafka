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
import io.confluent.kafka.server.plugins.auth.PlainSaslServer;
import io.confluent.security.authorizer.AclAccessRule;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.provider.ConfluentAuthorizationEvent;
import io.confluent.security.rbac.RbacAccessRule;
import io.confluent.security.rbac.RoleBinding;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.PlaintextAuthenticationContext;
import org.apache.kafka.common.security.auth.SaslAuthenticationContext;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.server.audit.AuditEventStatus;
import org.apache.kafka.server.audit.AuthenticationErrorInfo;
import org.apache.kafka.server.audit.AuthenticationEvent;

import javax.net.ssl.SSLSession;
import javax.security.sasl.SaslServer;
import java.util.Optional;

public class AuditLogUtils {
  public final static String AUTHENTICATION_FAILED_EVENT_USER = "None:UNKNOWN_USER";
  public static final String AUTHENTICATION_EVENT_NAME = "kafka.Authentication";

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
        .setMethodName(AUTHENTICATION_EVENT_NAME)
        .setResourceName(source);

    //set authenticationInfo
    AuthenticationInfo.Builder authenticationBuilder = AuthenticationInfo.newBuilder();
    if (authenticationEvent.principal().isPresent()) {
      KafkaPrincipal principal = authenticationEvent.principal().get();
      authenticationBuilder.setPrincipal(principal.getPrincipalType() + ":" + principal.getName());
    } else {
      // for auth failure events set principal as unknown user
      authenticationBuilder.setPrincipal(AUTHENTICATION_FAILED_EVENT_USER);
    }

    // set authentication metadata
    AuthenticationMetadata.Builder metadataBuilder = authenticationBuilder.getMetadataBuilder();
    metadataBuilder.setIdentifier(getIdentifier(authenticationEvent));
    metadataBuilder.setMechanism(getMechanism(authenticationEvent.authenticationContext()));
    authenticationBuilder.setMetadata(metadataBuilder.build());

    builder.setAuthenticationInfo(authenticationBuilder);

    //set status
    Result.Builder resultBuilder = Result.newBuilder();
    resultBuilder.setStatus(authenticationEvent.status().name());
    if (authenticationEvent.status() != AuditEventStatus.SUCCESS) {
      authenticationEvent.authenticationException().ifPresent(e ->
          resultBuilder.setMessage(prepareErrorMessage(e)));
    }

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

  private static String prepareErrorMessage(final AuthenticationException authenticationException) {
    AuthenticationErrorInfo errorInfo = authenticationException.errorInfo();
    if (!errorInfo.errorMessage().isEmpty())
      return errorInfo.errorMessage();

    return authenticationException.getAuditMessage();
  }

  /**
   * This returns the auth mechanism string as "SSL", "SASL_PLAINTEXT", "SASL_SSL/PLAIN",
   * "SASL_SSL/OAUTHBEARER", "SASL_SSL/GSSAPI" etc.
   */
  private static String getMechanism(final AuthenticationContext context) {
    if (context instanceof PlaintextAuthenticationContext || context instanceof SslAuthenticationContext) {
      return context.securityProtocol().name;
    } else if (context instanceof SaslAuthenticationContext) {
      SaslServer saslServer = ((SaslAuthenticationContext) context).server();
      return context.securityProtocol().name + "/" +  saslServer.getMechanismName();
    } else {
      return "";
    }
  }

  private static String getIdentifier(final AuthenticationEvent event) {
    AuthenticationContext context = event.authenticationContext();
    if (context instanceof SslAuthenticationContext) {
      return sslPeerPrincipal((SslAuthenticationContext) context);
    } else if (context instanceof SaslAuthenticationContext) {
      return saslIdentifier(event);
    } else {
      return "";
    }
  }

  private static String saslIdentifier(final AuthenticationEvent authenticationEvent) {
    SaslAuthenticationContext context = (SaslAuthenticationContext) authenticationEvent.authenticationContext();
    // handle if any authentication exception
    Optional<AuthenticationException> exceptionOptional =  authenticationEvent.authenticationException();
    if (exceptionOptional.isPresent()) {
      AuthenticationErrorInfo errorInfo = exceptionOptional.get().errorInfo();
      return errorInfo.identifier();
    } else { //handle authentication success events
      SaslServer saslServer = context.server();
      if (saslServer instanceof PlainSaslServer) {
        PlainSaslServer server = (PlainSaslServer) saslServer;
        return server.userIdentifier();
      } else {
        return saslServer.getAuthorizationID();
      }
    }
  }

  private static String sslPeerPrincipal(final SslAuthenticationContext context) {
    try {
      SSLSession sslSession = context.session();
      return sslSession.getPeerPrincipal().getName();
    } catch (Exception exception) {
      return "";
    }
  }
}
