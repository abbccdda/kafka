// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.audit.provider;

import io.confluent.security.audit.CloudEvent;
import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.audit.EventLogger;
import io.confluent.security.audit.appender.KafkaEventAppender;
import io.confluent.security.audit.appender.LogEventAppender;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.provider.AuditLogProvider;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.ConfigException;

public class ConfluentAuditLogProvider implements AuditLogProvider {

  private static final String DEFAULT_LOGGER = "default.logger";
  private static final String KAFKA_LOGGER = "kafka.logger";

  // Default appender that is used if audit logging to Kafka topic fails or if events are not generated
  private EventLogger localFileLogger;
  private EventLogger kafkaLogger;

  private volatile boolean ready;

  @Override
  public void configure(Map<String, ?> configs) {
    Map<String, Object> fileConfigs = new HashMap<>(configs);
    fileConfigs.put(EventLogConfig.EVENT_LOGGER_CLASS_CONFIG, LogEventAppender.class.getName());
    localFileLogger = EventLogger.logger(DEFAULT_LOGGER, fileConfigs);

    Map<String, Object> kafkaConfigs = new HashMap<>(configs);
    kafkaConfigs.put(EventLogConfig.EVENT_LOGGER_CLASS_CONFIG, KafkaEventAppender.class.getName());
    kafkaLogger = EventLogger.logger(KAFKA_LOGGER, kafkaConfigs);
    this.ready = true;
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    Set<String> configs = new HashSet<>();
    configs.addAll(localFileLogger.reconfigurableConfigs());
    configs.addAll(kafkaLogger.reconfigurableConfigs());
    return configs;
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    localFileLogger.validateReconfiguration(configs);
    kafkaLogger.validateReconfiguration(configs);
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    localFileLogger.reconfigure(configs);
    kafkaLogger.reconfigure(configs);
  }

  @Override
  public CompletionStage<Void> start(Map<String, ?> interBrokerListenerConfigs) {
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String providerName() {
    return "CONFLUENT";
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    // Even if audit logger publishes audit events to this Kafka cluster, return false.
    // In all cases, we fallback to backup provider and log locally to file if the destination
    // cluster is not ready.
    return false;
  }

  @Override
  public boolean needsLicense() {
    return true;
  }

  @Override
  public boolean providerConfigured(Map<String, ?> configs) {
    return configs.containsKey(EventLogConfig.EVENT_LOGGER_PREFIX + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
  }

  @Override
  public void log(RequestContext requestContext,
                  Action action,
                  AuthorizeResult authorizeResult,
                  AuthorizePolicy authorizePolicy) {

    boolean generateEvent;
    switch (authorizeResult) {
      case ALLOWED:
        generateEvent = action.logIfAllowed();
        break;
      case DENIED:
        generateEvent = action.logIfDenied();
        break;
      default:
        generateEvent = true;
        break;
    }
    EventLogger logger = ready && generateEvent ? kafkaLogger : localFileLogger;
    logger.log(newCloudEvent(requestContext, action, authorizeResult, authorizePolicy));
  }

  @Override
  public void close() throws IOException {
  }

  protected EventLogger localFileLogger() {
    return localFileLogger;
  }

  protected EventLogger kafkaLogger() {
    return kafkaLogger;
  }

  private CloudEvent newCloudEvent(RequestContext requestContext,
                                   Action action,
                                   AuthorizeResult authorizeResult,
                                   AuthorizePolicy authorizePolicy) {

    // TODO: Create audit log event
    return CloudEvent.newBuilder()
        .build();
  }
}
