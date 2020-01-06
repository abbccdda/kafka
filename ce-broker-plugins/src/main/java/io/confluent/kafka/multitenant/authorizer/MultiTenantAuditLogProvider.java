/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.kafka.multitenant.authorizer;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.multitenant.TenantMetadata;
import io.confluent.kafka.multitenant.utils.TenantView;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.AuditLogProvider;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.ConfigException;

public class MultiTenantAuditLogProvider implements AuditLogProvider {

  private final AuditLogProvider delegateAuditLogProvider;

  public MultiTenantAuditLogProvider(
      AuditLogProvider delegateAuditLogProvider) {
    this.delegateAuditLogProvider = delegateAuditLogProvider;
  }

  @Override
  public void logAuthorization(Scope sourceScope, RequestContext requestContext, Action action,
      AuthorizeResult authorizeResult, AuthorizePolicy authorizePolicy) {

    if (requestContext.principal() instanceof MultiTenantPrincipal) {
      TenantMetadata metadata = ((MultiTenantPrincipal) requestContext.principal())
          .tenantMetadata();
      RequestContext tenantRequestContext = TenantView.tenantRequestContext(requestContext);
      Scope tenantScope = TenantView.tenantScope(action.scope(), metadata.clusterId);
      Scope tenantSourceScope = TenantView.tenantScope(sourceScope, metadata.clusterId);
      ResourcePattern tenantResourcePattern = TenantView
          .tenantResourcePattern(action.resourcePattern(),
              metadata.tenantPrefix());
      Action tenantAction = new Action(tenantScope, tenantResourcePattern, action.operation(),
          action.resourceReferenceCount(), action.logIfAllowed(), action.logIfDenied());
      AuthorizePolicy tenantAuthorizePolicy = TenantView.tenantAuthorizePolicy(authorizePolicy,
          metadata.tenantPrefix());
      delegateAuditLogProvider
          .logAuthorization(tenantSourceScope, tenantRequestContext, tenantAction, authorizeResult,
              tenantAuthorizePolicy);
    } else {
      delegateAuditLogProvider
          .logAuthorization(sourceScope, requestContext, action, authorizeResult, authorizePolicy);
    }
  }

  @Override
  public boolean providerConfigured(Map<String, ?> configs) {
    return delegateAuditLogProvider.providerConfigured(configs);
  }

  @Override
  public String providerName() {
    return "MULTI_TENANT_" + delegateAuditLogProvider.providerName();
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    return delegateAuditLogProvider.usesMetadataFromThisKafkaCluster();
  }

  @Override
  public void close() throws IOException {
    delegateAuditLogProvider.close();
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return delegateAuditLogProvider.reconfigurableConfigs();
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    delegateAuditLogProvider.validateReconfiguration(configs);
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    delegateAuditLogProvider.reconfigure(configs);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    delegateAuditLogProvider.configure(configs);
  }
}
