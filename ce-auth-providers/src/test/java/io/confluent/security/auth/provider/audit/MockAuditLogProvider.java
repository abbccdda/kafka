// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.audit;

import static org.junit.Assert.assertTrue;

import io.confluent.security.authorizer.provider.AuthorizationLogData;
import io.confluent.security.authorizer.provider.DefaultAuditLogProvider;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.kafka.common.config.ConfigException;

public class MockAuditLogProvider extends DefaultAuditLogProvider {

  public static final String AUDIT_LOG_SIZE_CONFIG = "mock.audit.log.size";
  public static final Queue<AuthorizationLogData> AUDIT_LOG = new ConcurrentLinkedQueue<>();

  public static volatile int logSize;

  static void clear() {
    AUDIT_LOG.clear();
  }

  @Override
  public String providerName() {
    return "MOCK";
  }

  @Override
  public boolean providerConfigured(Map<String, ?> configs) {
    return configs.containsKey(AUDIT_LOG_SIZE_CONFIG);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    logSize = Integer.parseInt((String) configs.get(AUDIT_LOG_SIZE_CONFIG));
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return Collections.singleton(AUDIT_LOG_SIZE_CONFIG);
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    String value = (String) configs.get(AUDIT_LOG_SIZE_CONFIG);
    if (value == null)
      throw new ConfigException("Mandatory config not found: " + AUDIT_LOG_SIZE_CONFIG);
    Integer.parseInt((String) configs.get(AUDIT_LOG_SIZE_CONFIG));
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    logSize = Integer.parseInt((String) configs.get(AUDIT_LOG_SIZE_CONFIG));
  }

  @Override
  public void logAuthorization(AuthorizationLogData data) {
    assertTrue("Too many log entries:" + AUDIT_LOG, AUDIT_LOG.size() < logSize);
    if (sanitizer != null) {
      data = sanitizer.apply(data);
    }
    AUDIT_LOG.add(data);
  }

}
