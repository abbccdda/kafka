/*
 * Copyright [2019 - 2020] Confluent Inc.
 */
package io.confluent.kafka.security.authorizer;

import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.AuditLogProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.test.TestUtils;

public class MockAuditLogProvider implements AuditLogProvider {

  public static volatile MockAuditLogProvider instance;
  public final List<MockAuditLogEntry> auditLog = new ArrayList<>();
  private final ArrayList<String> states = new ArrayList<>();
  private boolean fail = false;

  public MockAuditLogProvider() {
    instance = this;
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return Collections.emptySet();
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
  }

  @Override
  public void configure(Map<String, ?> configs) {
    states.add("configured");
  }

  @Override
  public CompletionStage<Void> start(AuthorizerServerInfo serverInfo,
      Map<String, ?> interBrokerListenerConfigs) {
    states.add("started");
    // Return incomplete future to ensure authorizer is not blocked by audit logger
    return new CompletableFuture<>();
  }

  @Override
  public boolean providerConfigured(Map<String, ?> configs) {
    if (configs.containsKey("confluent.security.event.logger.enable")) {
      return !"false".equals(configs.get("confluent.security.event.logger.enable"));
    }
    return true;
  }

  @Override
  public String providerName() {
    return "MOCK_AUDIT";
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    return false;
  }

  @Override
  public void logAuthorization(Scope sourceScope, RequestContext requestContext, Action action,
      AuthorizeResult authorizeResult, AuthorizePolicy authorizePolicy) {
    if (fail) {
      throw new RuntimeException("MockAuditLogProvider intentional failure");
    }
    if (action.logIfAllowed() && authorizeResult == AuthorizeResult.ALLOWED ||
        action.logIfDenied() && authorizeResult == AuthorizeResult.DENIED) {
      auditLog.add(new MockAuditLogEntry(sourceScope, requestContext, action, authorizeResult,
          authorizePolicy));
    }
  }

  @Override
  public void close() {
  }

  MockAuditLogEntry lastEntry() {
    return auditLog.get(auditLog.size() - 1);
  }

  void ensureStarted() throws Exception {
    TestUtils.waitForCondition(() -> states.equals(Arrays.asList("configured", "started")),
        "Audit log provider not started, states=" + states);
  }

  public static void reset() {
    instance = null;
  }

  void setFail(boolean fail) {
    this.fail = fail;
  }
}
