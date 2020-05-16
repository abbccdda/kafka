// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import io.confluent.security.authorizer.AuthorizeResult;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.server.audit.AuditEvent;
import org.apache.kafka.server.audit.AuditEventType;
import org.apache.kafka.server.audit.AuditLogProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultAuditLogProvider implements AuditLogProvider {

  protected static final Logger log = LoggerFactory.getLogger("kafka.authorizer.logger");
  protected UnaryOperator<AuditEvent> sanitizer;

  @Override
  public void configure(Map<String, ?> configs) {
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
  public CompletionStage<Void> start(Map<String, ?> interBrokerListenerConfigs) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    return false;
  }

  @Override
  public boolean providerConfigured(Map<String, ?> configs) {
    return true;
  }

  @Override
  public void setSanitizer(UnaryOperator<AuditEvent> sanitizer) {
    this.sanitizer = sanitizer;
  }

  @Override
  public void logEvent(final AuditEvent auditEvent) {
    if (auditEvent.type() == AuditEventType.AUTHORIZATION) {
      logAuthorization(auditEvent);
    }
  }

  /**
   * Log using the same format as AK AclAuthorizer:
   * <pre>
   *  def logMessage: String = {
   *    val authResult = if (authorized) "Allowed" else "Denied"
   *    s"Principal = $principal is $authResult Operation = $operation from host = $host on
   * resource
   * = $resource"
   *  }
   * </pre>
   */
  private void logAuthorization(AuditEvent auditEvent) {
    if (sanitizer != null) {
      auditEvent = sanitizer.apply(auditEvent);
    }
    ConfluentAuthorizationEvent authZEvent = (ConfluentAuthorizationEvent) auditEvent;
    String logMessage = "Principal = {} is {} Operation = {} from host = {} on resource = {}";
    KafkaPrincipal principal = authZEvent.requestContext().principal();
    String host = authZEvent.requestContext().clientAddress().getHostAddress();
    String operation = authZEvent.action().operation().name();
    String resource = SecurityUtils.toPascalCase(authZEvent.action().resourceType().name()) + ":" +
        authZEvent.action().resourcePattern().patternType() + ":" +
        authZEvent.action().resourceName();
    if (authZEvent.authorizeResult() == AuthorizeResult.ALLOWED) {
      if (authZEvent.action().logIfAllowed())
        log.debug(logMessage, principal, "Allowed", operation, host, resource);
      else
        log.trace(logMessage, principal, "Allowed", operation, host, resource);
    } else {
      if (authZEvent.action().logIfDenied())
        log.info(logMessage, principal, "Denied", operation, host, resource);
      else
        log.trace(logMessage, principal, "Denied", operation, host, resource);
    }
  }

  @Override
  public void close() throws IOException {
  }
}
