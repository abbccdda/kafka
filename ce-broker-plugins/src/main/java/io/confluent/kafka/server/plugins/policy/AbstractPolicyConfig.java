// (Copyright) [2020 - 2020] Confluent, Inc.
package io.confluent.kafka.server.plugins.policy;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.PolicyViolationException;

public abstract class AbstractPolicyConfig extends AbstractConfig {

  protected AbstractPolicyConfig(ConfigDef configDef, Map<String, ?> clientConfigs) {
    super(configDef, clientConfigs);
  }

  protected final void checkPolicyMax(Map<String, String> configs, String maxPolicyConfigProperty,
      String configProperty) {
    if (configs.containsKey(configProperty)) {
      long configuredValue = Long.parseLong(configs.get(configProperty));
      long policyMax = ((Number) get(maxPolicyConfigProperty)).longValue();
      if (configuredValue > policyMax) {
        throw new PolicyViolationException(
            String.format("Config property '%s' with value '%d' exceeded max limit of %d.",
                configProperty, configuredValue, policyMax));
      }
    }
  }

  protected final void checkPolicyMin(Map<String, String> configs, String minPolicyConfigProperty,
      String configProperty) {
    if (configs.containsKey(configProperty)) {
      long configuredValue = Long.parseLong(configs.get(configProperty));
      long policyMin = ((Number) get(minPolicyConfigProperty)).longValue();
      if (configuredValue < policyMin) {
        throw new PolicyViolationException(
            String.format("Config property '%s' with value '%d' exceeded min limit of %d.",
                configProperty, configuredValue, policyMin));
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected final List<String> parseList(Map<String, String> configs, String configName) {
    String configValue = configs.get(configName);
    if (configValue == null)
      return null;
    try {
      return (List<String>) ConfigDef.parseType(configName, configValue, Type.LIST);
    } catch (ConfigException e) {
      throw new PolicyViolationException(e.getMessage());
    }
  }

}
