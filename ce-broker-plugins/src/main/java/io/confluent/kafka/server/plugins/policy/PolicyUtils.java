// (Copyright) [2020 - 2020] Confluent, Inc.
package io.confluent.kafka.server.plugins.policy;

import java.util.Map;
import java.util.function.Predicate;
import org.apache.kafka.common.errors.PolicyViolationException;

public class PolicyUtils {

  private PolicyUtils() {}

  protected final static void validateConfigsAreUpdatable(Map<String, String> configs,
      Predicate<String> predicate) {
    for (Map.Entry<String, String> entry : configs.entrySet()) {
      String configName = entry.getKey();
      if (!predicate.test(configName))
        throw new PolicyViolationException("Altering config property '" + configName +
            "' is disallowed.");
    }
  }

}
