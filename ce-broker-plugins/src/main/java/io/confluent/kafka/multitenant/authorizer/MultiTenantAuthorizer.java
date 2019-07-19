// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.multitenant.authorizer;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import io.confluent.kafka.security.authorizer.ConfluentServerAuthorizer;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import java.util.HashMap;
import java.util.Map;
import kafka.security.auth.Acl;
import kafka.security.auth.Resource;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import scala.collection.JavaConversions;
import scala.collection.immutable.Set;

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
  public void addAcls(Set<Acl> acls, Resource resource) {
    checkAclsEnabled();
    if (acls.isEmpty()) {
      return;
    }

    // Sanity check tenant ACLs. All tenant ACLs have principal containing tenant prefix
    // and resource names starting with tenant prefix. Also verify that the total number
    // of acls for the tenant doesn't exceed the configured maximum after this add.
    //
    // Note: we are also assuming that there will be no ACLs for tenant resources
    // with non-tenant principals (e.g broker ACLs will not specify tenant resource names)
    // We don't have a way to verify this, but describe/delete filters rely on this assumption.
    String firstTenantPrefix = null;
    KafkaPrincipal firstPrincipal = acls.head().principal();
    if (MultiTenantPrincipal.isTenantPrincipal(firstPrincipal)) {
      firstTenantPrefix = tenantPrefix(firstPrincipal.getName());
      if (!resource.name().startsWith(firstTenantPrefix)) {
        log.error("Unexpected ACL request for resource {} without tenant prefix {}",
            resource, firstTenantPrefix);
        throw new IllegalStateException("Internal error: Could not create ACLs for " + resource);
      }
      if (maxAclsPerTenant != Integer.MAX_VALUE
          && acls.size() + tenantAclCount(firstTenantPrefix) > maxAclsPerTenant) {
        throw new InvalidRequestException("ACLs not created since it will exceed the limit "
            + maxAclsPerTenant);
      }
    }

    final String tenantPrefix = firstTenantPrefix;
    java.util.Set<Acl> aclsToAdd = JavaConversions.setAsJavaSet(acls);
    if (aclsToAdd.stream().anyMatch(acl -> !inScope(acl.principal(), tenantPrefix))) {
      log.error("ACL requests contain invalid tenant principal {}", aclsToAdd);
      throw new IllegalStateException("Internal error: Could not create ACLs for " + resource);
    }

    super.addAcls(acls, resource);
  }

  @Override
  public boolean removeAcls(scala.collection.immutable.Set<Acl> aclsTobeRemoved, Resource resource) {
    checkAclsEnabled();
    return super.removeAcls(aclsTobeRemoved, resource);
  }

  @Override
  public boolean removeAcls(Resource resource) {
    checkAclsEnabled();
    return super.removeAcls(resource);
  }

  @Override
  public scala.collection.immutable.Set<Acl> getAcls(Resource resource) {
    checkAclsEnabled();
    return super.getAcls(resource);
  }

  @Override
  public scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> getAcls(KafkaPrincipal principal) {
    checkAclsEnabled();
    return super.getAcls(principal);
  }

  @Override
  public scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> getAcls() {
    checkAclsEnabled();
    return super.getAcls();
  }

  private String tenantPrefix(String name) {
    int index = name.indexOf(MultiTenantPrincipal.DELIMITER);
    if (index == -1) {
      throw new InvalidRequestException("Invalid tenant principal in ACL: " + name);
    } else {
      return name.substring(0, index + 1);
    }
  }

  // Check whether `principal` is within the same tenant (or non-tenant) scope
  // If `tenantPrefix` is non-null, principal must be a tenant principal with
  // the same prefix since ACL requests cannot contain ACLs of multiple tenants.
  // If `tenantPrefix` is null, principal must not be a tenant principal since
  // requests on listeners without the tenant interceptor are not allowed to
  // access tenant ACLs.
  private boolean inScope(KafkaPrincipal principal, String tenantPrefix) {
    if (tenantPrefix != null && !tenantPrefix.isEmpty()) {
      return MultiTenantPrincipal.isTenantPrincipal(principal)
          && principal.getName().startsWith(tenantPrefix);
    } else {
      return !MultiTenantPrincipal.isTenantPrincipal(principal);
    }
  }

  private long tenantAclCount(String tenantPrefix) {
    return JavaConversions.asJavaCollection(getAcls().values()).stream()
        .flatMap(acls -> JavaConversions.asJavaCollection(acls).stream())
        .filter(acl -> inScope(acl.principal(), tenantPrefix))
        .count();
  }

  private void checkAclsEnabled() {
    if (authorizationDisabled) {
      throw new InvalidRequestException("ACLs are not enabled on this broker");
    }
  }
}
