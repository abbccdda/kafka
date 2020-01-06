/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.kafka.multitenant.utils;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.security.authorizer.AclAccessRule;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import java.net.InetAddress;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.SecurityUtils;

/**
 * These functions transform various pieces of internal information to remove internal
 * implementation details. The result is a view of these objects that is safe to display to the
 * tenant
 */
public class TenantView {


  private static KafkaPrincipal tenantPrincipal(MultiTenantPrincipal principal) {
    return new KafkaPrincipal(KafkaPrincipal.USER_TYPE, principal.user());
  }

  private static KafkaPrincipal tenantPrincipal(KafkaPrincipal principal) {
    if (MultiTenantPrincipal.TENANT_USER_TYPE.equals(principal.getPrincipalType())) {
      int index = principal.getName().indexOf(MultiTenantPrincipal.DELIMITER);
      if (index > 0) {
        return new KafkaPrincipal(KafkaPrincipal.USER_TYPE,
            principal.getName().substring(index + 1));
      }
    }
    throw new NotTenantPrefixedException("Expected a multi-tenant principal: " + principal);
  }

  public static Scope tenantScope(Scope scope, String tenantClusterId) {
    Scope.Builder builder = new Scope.Builder();
    scope.path().forEach(builder::addPath);
    scope.clusters().forEach((k, v) -> {
      if (k.equals(Scope.KAFKA_CLUSTER_TYPE)) {
        builder.withKafkaCluster(tenantClusterId);
      } else {
        builder.withCluster(k, v);
      }
    });
    return builder.build();
  }

  public static ResourcePattern tenantResourcePattern(
      ResourcePattern resourcePattern, String tenantPrefix) {
    if (!resourcePattern.name().startsWith(tenantPrefix)) {
      throw new NotTenantPrefixedException(
          "Expected a multi-tenant prefix: " + resourcePattern.name());
    }
    return new ResourcePattern(
        resourcePattern.resourceType(),
        resourcePattern.name().substring(tenantPrefix.length()),
        resourcePattern.patternType());
  }

  public static RequestContext tenantRequestContext(RequestContext requestContext) {
    return new RequestContext() {
      @Override
      public KafkaPrincipal principal() {
        return tenantPrincipal((MultiTenantPrincipal) requestContext.principal());
      }

      @Override
      public String requestSource() {
        return requestContext.requestSource();
      }

      @Override
      public String listenerName() {
        return requestContext.listenerName();
      }

      @Override
      public SecurityProtocol securityProtocol() {
        return requestContext.securityProtocol();
      }

      @Override
      public InetAddress clientAddress() {
        return requestContext.clientAddress();
      }

      @Override
      public int requestType() {
        return requestContext.requestType();
      }

      @Override
      public int requestVersion() {
        return requestContext.requestVersion();
      }

      @Override
      public String clientId() {
        return requestContext.clientId();
      }

      @Override
      public int correlationId() {
        return requestContext.correlationId();
      }
    };

  }

  private static AccessControlEntry tenantAccessControlEntry(AccessControlEntry entry) {
    return new AccessControlEntry(
        tenantPrincipal(SecurityUtils.parseKafkaPrincipal(entry.principal())).toString(),
        entry.host(), entry.operation(), entry.permissionType()
    );
  }

  private static AclAccessRule tenantAclAccessRule(AclAccessRule accessRule, String tenantPrefix) {
    AclBinding aclBinding = accessRule.aclBinding();
    AclBinding tenantAclBinding = new AclBinding(
        ResourcePattern.to(tenantResourcePattern(ResourcePattern.from(aclBinding.pattern()),
            tenantPrefix)), tenantAccessControlEntry(aclBinding.entry()));
    return new AclAccessRule(
        tenantResourcePattern(accessRule.resourcePattern(), tenantPrefix),
        tenantPrincipal(accessRule.principal()),
        accessRule.permissionType(),
        accessRule.host(),
        accessRule.operation(),
        accessRule.policyType(),
        tenantAclBinding);
  }

  public static AuthorizePolicy tenantAuthorizePolicy(AuthorizePolicy authorizePolicy,
      String tenantPrefix) {
    switch (authorizePolicy.policyType()) {
      case DENY_ACL:
      case ALLOW_ACL:
        return tenantAclAccessRule((AclAccessRule) authorizePolicy, tenantPrefix);
      case ALLOW_ROLE:
        // Note: We don't currently support Multi-tenant RBAC
        throw new RuntimeException("Tenant RBAC is not yet supported");
      default:
        return authorizePolicy;
    }
  }

  public static class NotTenantPrefixedException extends RuntimeException {

    public NotTenantPrefixedException(String message) {
      super(message);
    }
  }

}
