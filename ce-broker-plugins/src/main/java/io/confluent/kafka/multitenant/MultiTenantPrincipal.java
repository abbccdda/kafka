// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class MultiTenantPrincipal extends KafkaPrincipal {

  public static final String TENANT_USER_TYPE = "TenantUser";
  public static final String TENANT_WILDCARD_USER_TYPE = MultiTenantPrincipal.TENANT_USER_TYPE
      + "*";

  private final String user;
  private final TenantMetadata tenantMetadata;

  public MultiTenantPrincipal(String user, TenantMetadata tenantMetadata) {
    super(TENANT_USER_TYPE, tenantMetadata.tenantPrefix()  + user);
    this.user = user;
    this.tenantMetadata = tenantMetadata;
  }

  public TenantMetadata tenantMetadata() {
    return tenantMetadata;
  }

  public String user() {
    return user;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    MultiTenantPrincipal that = (MultiTenantPrincipal) o;
    return tenantMetadata != null ? tenantMetadata.equals(that.tenantMetadata) :
        that.tenantMetadata == null;
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (tenantMetadata != null ? tenantMetadata.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "MultiTenantPrincipal("
        + "tenantMetadata=" + tenantMetadata + ", "
        + "user=" + user + ")";
  }

  public static final boolean isTenantPrincipal(KafkaPrincipal principal) {
    return principal.getPrincipalType().startsWith(TENANT_USER_TYPE);
  }

}
