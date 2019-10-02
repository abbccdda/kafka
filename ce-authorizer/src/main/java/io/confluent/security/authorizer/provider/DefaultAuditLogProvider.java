// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultAuditLogProvider implements AuditLogProvider {

  protected static final Logger log = LoggerFactory.getLogger("kafka.authorizer.logger");

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
  public String providerName() {
    return "DEFAULT";
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    return false;
  }

  @Override
  public boolean needsLicense() {
    return false;
  }

  @Override
  public boolean providerConfigured(Map<String, ?> configs) {
    return true;
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
  @Override
  public void logAuthorization(RequestContext requestContext,
                  Action action,
                  AuthorizeResult authorizeResult,
                  AuthorizePolicy authorizePolicy) {
    String logMessage = "Principal = {} is {} Operation = {} from host = {} on resource = {}";
    KafkaPrincipal principal = requestContext.principal();
    String host = requestContext.clientAddress().getHostAddress();
    String operation = action.operation().name();
    String resource = SecurityUtils.toPascalCase(action.resourceType().name()) + ":" +
        action.resourcePattern().patternType() + ":" +
        action.resourceName();
    if (authorizeResult == AuthorizeResult.ALLOWED) {
      if (action.logIfAllowed())
        log.debug(logMessage, principal, "Allowed", operation, host, resource);
      else
        log.trace(logMessage, principal, "Allowed", operation, host, resource);
    } else {
      if (action.logIfDenied())
        log.info(logMessage, principal, "Denied", operation, host, resource);
      else
        log.trace(logMessage, principal, "Denied", operation, host, resource);
    }
  }

  @Override
  public void close() throws IOException {
  }
}
