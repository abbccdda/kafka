/*
 * Copyright [2019 - 2020] Confluent Inc.
 */
package io.confluent.kafka.security.authorizer;

import io.confluent.kafka.security.audit.event.ConfluentAuthenticationEvent;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.ConfluentAuthorizationEvent;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.server.audit.AuditEvent;
import org.apache.kafka.server.audit.AuditEventType;
import org.apache.kafka.server.audit.AuditLogProvider;
import org.apache.kafka.server.audit.AuthenticationEvent;
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

public class MockAuditLogProvider implements AuditLogProvider, ClusterResourceListener {

  public static volatile MockAuditLogProvider instance;
  public final List<ConfluentAuthorizationEvent> authorizationLog = new ArrayList<>();
  public final List<AuthenticationEvent> authenticationLog = new ArrayList<>();
  private final ArrayList<String> states = new ArrayList<>();
  private boolean fail = false;
  private UnaryOperator<AuditEvent> santizer;
  private Scope scope;

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
    if (fail) {
      throw new RuntimeException("MockAuditLogProvider intentional failure");
    }

    if (auditEvent.type() == AuditEventType.AUTHORIZATION) {
      handleAuthorizationEvent((ConfluentAuthorizationEvent) auditEvent);
    } else if (auditEvent.type() == AuditEventType.AUTHENTICATION) {
      handleAuthenticationEvent((AuthenticationEvent) auditEvent);
    } else {
      throw new UnsupportedOperationException("Event type is not supported: " + auditEvent.type());
    }
  }

  private void handleAuthenticationEvent(final AuthenticationEvent auditEvent) {
    ConfluentAuthenticationEvent authenticationEvent;
    if (auditEvent instanceof ConfluentAuthenticationEvent) {
      authenticationEvent = (ConfluentAuthenticationEvent) auditEvent;
    } else {
      authenticationEvent = new ConfluentAuthenticationEvent(auditEvent, scope);
    }

    if (santizer != null) {
      authenticationEvent = (ConfluentAuthenticationEvent) santizer.apply(authenticationEvent);
    }
    authenticationLog.add(authenticationEvent);
  }

  private void handleAuthorizationEvent(ConfluentAuthorizationEvent authZEvent) {
    if (santizer != null) {
      authZEvent = (ConfluentAuthorizationEvent) santizer.apply(authZEvent);
    }
    if (authZEvent.action().logIfAllowed() && authZEvent.authorizeResult() == AuthorizeResult.ALLOWED ||
        authZEvent.action().logIfDenied() && authZEvent.authorizeResult() == AuthorizeResult.DENIED) {
      authorizationLog.add(authZEvent);
    }
  }

  @Override
  public void close() {
  }

  ConfluentAuthorizationEvent lastAuthorizationEntry() {
    return authorizationLog.get(authorizationLog.size() - 1);
  }

  public AuthenticationEvent lastAuthenticationEntry() {
    return authenticationLog.get(authenticationLog.size() - 1);
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

  @Override
  public void onUpdate(final ClusterResource clusterResource) {
    this.scope = Scope.kafkaClusterScope(clusterResource.clusterId());
  }
}
