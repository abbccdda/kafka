// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.server.plugins.auth;

import com.google.gson.annotations.SerializedName;
import java.util.Map;
import java.util.Objects;

class TenantConfig {

  @SerializedName("keys") final Map<String, KeyConfigEntry> apiKeys;

  TenantConfig(Map<String, KeyConfigEntry> apiKeys) {
    this.apiKeys = apiKeys;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TenantConfig that = (TenantConfig) o;
    return Objects.equals(apiKeys, that.apiKeys);
  }

  @Override
  public int hashCode() {
    return Objects.hash(apiKeys);
  }
}
