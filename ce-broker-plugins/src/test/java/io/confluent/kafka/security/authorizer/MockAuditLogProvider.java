/*
 * Copyright [2019 - 2020] Confluent Inc.
 */
package io.confluent.kafka.security.authorizer;

import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.provider.ConfluentAuthorizationEvent;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.audit.AuditEvent;
import org.apache.kafka.server.audit.AuditLogProvider;
import org.apache.kafka.test.TestUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;

public class MockAuditLogProvider implements AuditLogProvider {

  public static volatile MockAuditLogProvider instance;
  public final List<ConfluentAuthorizationEvent> auditLog = new ArrayList<>();
  private final ArrayList<String> states = new ArrayList<>();
  private boolean fail = false;
  private UnaryOperator<AuditEvent> santizer;

  public MockAuditLogProvider() {
    instance = this;
    states.add("configured");
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
  }

  @Override
  public CompletionStage<Void> start(Map<String, ?> interBrokerListenerConfigs) {
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
  public void setSanitizer(UnaryOperator<AuditEvent> sanitizer) {
    this.santizer = sanitizer;
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    return false;
  }

  @Override
  public void logEvent(AuditEvent auditEvent) {
    if (santizer != null) {
      auditEvent = santizer.apply(auditEvent);
    }
    ConfluentAuthorizationEvent authZEvent =  (ConfluentAuthorizationEvent) auditEvent;

    if (fail) {
      throw new RuntimeException("MockAuditLogProvider intentional failure");
    }
    if (authZEvent.action().logIfAllowed() && authZEvent.authorizeResult() == AuthorizeResult.ALLOWED ||
        authZEvent.action().logIfDenied() && authZEvent.authorizeResult() == AuthorizeResult.DENIED) {
      auditLog.add(authZEvent);
    }
  }

  @Override
  public void close() {
  }

  ConfluentAuthorizationEvent lastEntry() {
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
