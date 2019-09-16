// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.audit;

import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.provider.DefaultAuditLogProvider;
import java.util.Collections;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.kafka.common.config.ConfigException;

import static org.junit.Assert.assertTrue;

public class MockAuditLogProvider extends DefaultAuditLogProvider {

  public static final String AUDIT_LOG_SIZE_CONFIG = "mock.audit.log.size";
  public static final Queue<AuditLogEntry> AUDIT_LOG = new ConcurrentLinkedQueue<>();

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
  public void log(RequestContext requestContext,
                  Action action,
                  AuthorizeResult authorizeResult,
                  AuthorizePolicy authorizePolicy) {
    assertTrue("Too many log entries:" + AUDIT_LOG, AUDIT_LOG.size() < logSize);
    AUDIT_LOG.add(new AuditLogEntry(requestContext, action, authorizeResult, authorizePolicy));
  }

  public static class AuditLogEntry {
    public RequestContext requestContext;
    public Action action;
    public AuthorizeResult authorizeResult;
    public AuthorizePolicy authorizePolicy;

    AuditLogEntry(RequestContext requestContext,
                  Action action,
                  AuthorizeResult authorizeResult,
                  AuthorizePolicy authorizePolicy) {
      this.requestContext = requestContext;
      this.action = action;
      this.authorizeResult = authorizeResult;
      this.authorizePolicy = authorizePolicy;
    }

    @Override
    public String toString() {
      return "AuditLogEntry(" +
          "requestSource='" + requestContext.requestSource() + '\'' +
          ", principal='" + requestContext.principal() + '\'' +
          ", resource='" + action.resourcePattern() + '\'' +
          ", scope='" + action.scope() + '\'' +
          ", operation='" + action.operation() + '\'' +
          ", authorizeResult='" + authorizeResult + '\'' +
          ", authorizePolicy='" + authorizePolicy.policyType() + '\'' +
          ')';
    }
  }

}
