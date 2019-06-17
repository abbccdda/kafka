// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import io.confluent.security.authorizer.provider.GroupProvider;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class TestGroupProvider implements GroupProvider {

  static final String TEST_PROVIDER_PROP = "test.group.provider.config";
  static RuntimeException exception;
  static Map<KafkaPrincipal, Set<KafkaPrincipal>> groups = new HashMap<>();

  @Override
  public void configure(Map<String, ?> configs) {
  }

  @Override
  public Set<KafkaPrincipal> groups(KafkaPrincipal sessionPrincipal) {
    if (exception != null)
      throw exception;
    return groups.getOrDefault(sessionPrincipal, Collections.emptySet());
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    return false;
  }

  @Override
  public String providerName() {
    return "TEST";
  }

  @Override
  public boolean providerConfigured(Map<String, ?> configs) {
    return "TEST".equals(configs.get(TEST_PROVIDER_PROP));
  }

  @Override
  public void close() {
  }

  static void reset() {
    exception = null;
    groups.clear();
  }
}
