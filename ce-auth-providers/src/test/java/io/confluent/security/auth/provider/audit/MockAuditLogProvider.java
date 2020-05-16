// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.audit;

import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.confluent.security.authorizer.provider.ConfluentAuthorizationEvent;
import io.confluent.security.authorizer.provider.DefaultAuditLogProvider;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.audit.AuditEvent;

public class MockAuditLogProvider extends DefaultAuditLogProvider {

  public static final String AUDIT_LOG_SIZE_CONFIG = "mock.audit.log.size";
  public static final Queue<ConfluentAuthorizationEvent> AUDIT_LOG = new ConcurrentLinkedQueue<>();

  public static volatile int logSize;

  static void clear() {
    AUDIT_LOG.clear();
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
  public void logEvent(AuditEvent auditEvent) {
    assertTrue("Too many log entries:" + AUDIT_LOG, AUDIT_LOG.size() < logSize);
    if (sanitizer != null) {
      auditEvent = sanitizer.apply(auditEvent);
    }
    AUDIT_LOG.add((ConfluentAuthorizationEvent) auditEvent);
  }

}
