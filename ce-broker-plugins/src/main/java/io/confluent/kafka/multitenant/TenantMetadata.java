// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import io.confluent.kafka.multitenant.schema.TenantContext;
import java.util.Objects;

import io.confluent.kafka.multitenant.utils.Utils;

public class TenantMetadata {

  public final String tenantName;
  public final String clusterId;
  public final boolean allowDescribeBrokerConfigs;
  public final boolean isSuperUser;

  public TenantMetadata(String tenantName, String clusterId) {
    this(tenantName, clusterId, false);
  }

  public TenantMetadata(String tenantName, String clusterId, boolean allowDescribeBrokerConfigs) {
    this(tenantName, clusterId, allowDescribeBrokerConfigs, false);
  }

  TenantMetadata(String tenantName, String clusterId, boolean allowDescribeBrokerConfigs,
      boolean isSuperUser) {
    this.tenantName = Utils.requireNonEmpty(tenantName, "Tenant");
    this.clusterId = Utils.requireNonEmpty(clusterId, "ClusterId");
    this.allowDescribeBrokerConfigs = allowDescribeBrokerConfigs;
    this.isSuperUser = isSuperUser;
  }

  public String tenantPrefix() {
    return tenantName + TenantContext.DELIMITER;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TenantMetadata that = (TenantMetadata) o;
    return Objects.equals(tenantName, that.tenantName)
        && Objects.equals(clusterId, that.clusterId);

  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantName, clusterId);
  }

  @Override
  public String toString() {
    return "TenantMetadata(" +
            "tenantName='" + tenantName + "'" +
            ", clusterId='" + clusterId + "'" +
            ", allowDescribeBrokerConfigs=" + allowDescribeBrokerConfigs +
            ", isSuperUser=" + isSuperUser +
            ")";
  }

  public static class Builder {
    public final String tenantName;
    public final String clusterId;
    public boolean allowDescribeBrokerConfigs;
    public boolean isSuperUser;

    public Builder(String clusterId) {
      this.clusterId = clusterId;
      this.tenantName = clusterId;
    }

    public Builder allowDescribeBrokerConfigs() {
      this.allowDescribeBrokerConfigs = true;
      return this;
    }

    public Builder superUser(boolean isSuperUser) {
      this.isSuperUser = isSuperUser;
      return this;
    }

    public TenantMetadata build() {
      return new TenantMetadata(tenantName,
                                clusterId,
                                allowDescribeBrokerConfigs,
                                isSuperUser);
    }
  }
}
