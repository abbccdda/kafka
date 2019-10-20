// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.multitenant.authorizer;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.AuditLogProvider;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.security.authorizer.provider.GroupProvider;
import io.confluent.security.authorizer.provider.MetadataProvider;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

public class MultiTenantAuthorizer extends ConfluentServerAuthorizer {

  public static final String MAX_ACLS_PER_TENANT_PROP = "confluent.max.acls.per.tenant";
  public static final int DEFAULT_MAX_ACLS_PER_TENANT_PROP = 1000;
  static final int ACLS_DISABLED = 0;

  private int maxAclsPerTenant;
  private boolean authorizationDisabled;

  @Override
  public void configure(Map<String, ?> configs) {
    Map<String, Object>  authorizerConfigs = new HashMap<>(configs);
    String maxAcls = (String) configs.get(MAX_ACLS_PER_TENANT_PROP);
    maxAclsPerTenant = maxAcls != null ? Integer.parseInt(maxAcls) : DEFAULT_MAX_ACLS_PER_TENANT_PROP;
    authorizationDisabled = maxAclsPerTenant == ACLS_DISABLED;

    authorizerConfigs.put(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP,
        AccessRuleProviders.MULTI_TENANT.name());
    super.configure(authorizerConfigs);
  }

  @Override
  public List<? extends CompletionStage<AclCreateResult>> createAcls(
      AuthorizableRequestContext requestContext, List<AclBinding> aclBindings) {
    checkAclsEnabled();
    if (aclBindings.isEmpty()) {
      return Collections.emptyList();
    }

    // Sanity check tenant ACLs. All tenant ACLs have principal containing tenant prefix
    // and resource names starting with tenant prefix. Also verify that the total number
    // of acls for the tenant doesn't exceed the configured maximum after this add.
    //
    // Note: we are also assuming that there will be no ACLs for tenant resources
    // with non-tenant principals (e.g broker ACLs will not specify tenant resource names)
    // We don't have a way to verify this, but describe/delete filters rely on this assumption.
    String firstTenantPrefix = null;
    KafkaPrincipal firstPrincipal = SecurityUtils.parseKafkaPrincipal(aclBindings.get(0).entry().principal());
    if (MultiTenantPrincipal.isTenantPrincipal(firstPrincipal)) {
      firstTenantPrefix = tenantPrefix(firstPrincipal.getName());
      if (maxAclsPerTenant != Integer.MAX_VALUE
          && aclBindings.size() + tenantAclCount(firstTenantPrefix) > maxAclsPerTenant) {
        throw new InvalidRequestException("ACLs not created since it will exceed the limit "
            + maxAclsPerTenant);
      }
    }

    final String tenantPrefix = firstTenantPrefix;
    if (aclBindings.stream().anyMatch(acl -> !inScope(acl.entry().principal(), tenantPrefix))) {
      log.error("ACL requests contain invalid tenant principal {}", aclBindings);
      throw new IllegalStateException("Internal error: Could not create ACLs for " + aclBindings);
    }
    if (aclBindings.stream().anyMatch(acl -> !acl.pattern().name().startsWith(tenantPrefix))) {
      log.error("Unexpected ACL request for resources {} without tenant prefix {}",
          aclBindings, firstTenantPrefix);
      throw new IllegalStateException("Internal error: Could not create ACLs for " + aclBindings);
    }

    return super.createAcls(requestContext, aclBindings);
  }

  @Override
  public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(
      AuthorizableRequestContext requestContext, List<AclBindingFilter> aclBindingFilters) {
    checkAclsEnabled();
    return super.deleteAcls(requestContext, aclBindingFilters);
  }

  @Override
  public Iterable<AclBinding> acls(AclBindingFilter filter) {
    checkAclsEnabled();
    return super.acls(filter);
  }

  @Override
  protected void configureProviders(List<AccessRuleProvider> accessRuleProviders,
      GroupProvider groupProvider, MetadataProvider metadataProvider,
      AuditLogProvider auditLogProvider) {
    // Disable enable audit logger until multi-tenant audit logger for Cloud has been tested
    super.configureProviders(accessRuleProviders, groupProvider, metadataProvider, null);
  }

  private String tenantPrefix(String name) {
    int index = name.indexOf(MultiTenantPrincipal.DELIMITER);
    if (index == -1) {
      throw new InvalidRequestException("Invalid tenant principal in ACL: " + name);
    } else {
      return name.substring(0, index + 1);
    }
  }

  // Check whether `principalStr` is within the same tenant (or non-tenant) scope
  // If `tenantPrefix` is non-null, principal must be a tenant principal with
  // the same prefix since ACL requests cannot contain ACLs of multiple tenants.
  // If `tenantPrefix` is null, principal must not be a tenant principal since
  // requests on listeners without the tenant interceptor are not allowed to
  // access tenant ACLs.
  private boolean inScope(String principalStr, String tenantPrefix) {
    KafkaPrincipal principal = SecurityUtils.parseKafkaPrincipal(principalStr);
    if (tenantPrefix != null && !tenantPrefix.isEmpty()) {
      return MultiTenantPrincipal.isTenantPrincipal(principal)
          && principal.getName().startsWith(tenantPrefix);
    } else {
      return !MultiTenantPrincipal.isTenantPrincipal(principal);
    }
  }

  private long tenantAclCount(String tenantPrefix) {
    int count = 0;
    for (AclBinding binding : acls(AclBindingFilter.ANY)) {
      if (inScope(binding.entry().principal(), tenantPrefix))
        count++;
    }
    return count;
  }

  private void checkAclsEnabled() {
    if (authorizationDisabled) {
      throw new InvalidRequestException("ACLs are not enabled on this broker");
    }
  }
}
