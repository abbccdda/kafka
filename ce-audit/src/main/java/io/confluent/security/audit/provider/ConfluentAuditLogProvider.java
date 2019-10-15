// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.audit.provider;

import io.confluent.crn.ConfluentServerCrnAuthority;
import io.confluent.crn.CrnAuthorityConfig;
import io.confluent.crn.CrnSyntaxException;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.AuditLogUtils;
import io.confluent.security.audit.CloudEvent;
import io.confluent.security.audit.CloudEventUtils;
import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.audit.EventLogger;
import io.confluent.security.audit.appender.KafkaEventAppender;
import io.confluent.security.audit.appender.LogEventAppender;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.AuditLogProvider;
import io.confluent.security.authorizer.utils.ThreadUtils;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfluentAuditLogProvider implements AuditLogProvider, ClusterResourceListener {

  private static final String DEFAULT_LOGGER = "default.logger";
  private static final String KAFKA_LOGGER = "kafka.logger";

  public static final String AUTHORIZATION_MESSAGE_TYPE = "io.confluent.kafka.server/authorization";

  private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);

  protected static final Logger log = LoggerFactory.getLogger(ConfluentAuditLogProvider.class);

  // Default appender that is used if audit logging to Kafka topic fails or if events are not generated
  private EventLogger localFileLogger;
  private EventLogger kafkaLogger;
  private ExecutorService initExecutor;

  private ConfluentServerCrnAuthority crnAuthority;

  private volatile boolean kafkaLoggerStarted;
  private String clusterId;
  private Scope scope;

  @Override
  public void onUpdate(ClusterResource clusterResource) {
    this.clusterId = clusterResource.clusterId();
    this.scope = Scope.kafkaClusterScope(clusterId);
    this.scope.validate(false);
  }

  /**
   * The provider is configured and started during {@link #start(Map)} to avoid blocking
   * configure().
   */
  @Override
  public void configure(Map<String, ?> configs) {
    Map<String, Object> fileConfigs = new HashMap<>(configs);
    fileConfigs.put(EventLogConfig.EVENT_APPENDER_CLASS_CONFIG, LogEventAppender.class.getName());
    localFileLogger = EventLogger.logger(DEFAULT_LOGGER, fileConfigs);

    CrnAuthorityConfig crnAuthorityConfig = new CrnAuthorityConfig(configs);
    this.crnAuthority = new ConfluentServerCrnAuthority();
    this.crnAuthority.configure(crnAuthorityConfig.values());
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    Set<String> configs = new HashSet<>();
    configs.addAll(localFileLogger.reconfigurableConfigs());
    if (kafkaLogger != null) {
      configs.addAll(kafkaLogger.reconfigurableConfigs());
    }
    return configs;
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    localFileLogger.validateReconfiguration(configs);
    if (kafkaLogger != null) {
      kafkaLogger.validateReconfiguration(configs);
    }
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    localFileLogger.reconfigure(configs);
    if (kafkaLogger != null) {
      kafkaLogger.reconfigure(configs);
    }
  }

  @Override
  public CompletionStage<Void> start(Map<String, ?> interBrokerListenerConfigs) {
    initExecutor = Executors
        .newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("audit-init-%d", true));
    CompletableFuture<Void> future = new CompletableFuture<>();
    initExecutor.submit(() -> {
      try {
        Map<String, Object> kafkaConfigs = new HashMap<>(interBrokerListenerConfigs);
        kafkaConfigs
            .put(EventLogConfig.EVENT_APPENDER_CLASS_CONFIG, KafkaEventAppender.class.getName());
        kafkaLogger = eventLogger(kafkaConfigs);

        this.kafkaLoggerStarted = true;
        future.complete(null);
      } catch (Throwable e) {
        log.error("Audit log provider could not be started", e);
        future.completeExceptionally(e);
      }
    });
    return future.whenComplete((unused, e) -> initExecutor.shutdownNow());
  }

  @Override
  public String providerName() {
    return "CONFLUENT";
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    // If we can determine that none of the log destinations are in this cluster, we can return
    // false. Returning true for now as the safe option.
    return true;
  }

  @Override
  public boolean needsLicense() {
    return true;
  }

  @Override
  public boolean providerConfigured(Map<String, ?> configs) {
    EventLogConfig eventLogConfig = new EventLogConfig(configs);
    return eventLogConfig.getBoolean(EventLogConfig.EVENT_LOGGER_ENABLED_CONFIG);
  }

  @Override
  public void logAuthorization(RequestContext requestContext,
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
    EventLogger logger = kafkaLoggerStarted && generateEvent ? kafkaLogger : localFileLogger;
    logger.log(newCloudEvent(requestContext, action, authorizeResult, authorizePolicy));
  }

  @Override
  public void close() {
    if (initExecutor != null) {
      initExecutor.shutdownNow();
      try {
        initExecutor.awaitTermination(CLOSE_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        log.debug("ConfluentAuditLogProvider was interrupted while waiting to close");
        throw new InterruptException(e);
      }
    }
    Utils.closeQuietly(kafkaLogger, "kafkaLogger");
    Utils.closeQuietly(localFileLogger, "localFileLogger");
  }

  protected EventLogger localFileLogger() {
    return localFileLogger;
  }

  protected EventLogger kafkaLogger() {
    return kafkaLogger;
  }

  // Visibility for testing
  public ExecutorService initExecutor() {
    return initExecutor;
  }

  // Visibility for testing
  EventLogger eventLogger(Map<String, Object> configs) {
    return EventLogger.createLogger(configs);
  }

  private CloudEvent newCloudEvent(RequestContext requestContext, Action action,
      AuthorizeResult authorizeResult, AuthorizePolicy authorizePolicy) {

    try {
      String source = crnAuthority.canonicalCrn(scope).toString();
      String subject =
          crnAuthority.canonicalCrn(action.scope(), action.resourcePattern()).toString();

      AuditLogEntry entry = AuditLogUtils
          .authorizationEvent(source, subject, requestContext, action, authorizeResult,
              authorizePolicy);
      return CloudEventUtils.wrap(AUTHORIZATION_MESSAGE_TYPE, source, subject, entry);
    } catch (CrnSyntaxException e) {
      log.warn(
          "Couldn't create cloud event due to internally generated CRN syntax problem", e);
    }
    return null;
  }

  // Visibility for testing
  public boolean localFileLoggerReady() {
    return localFileLogger.ready();
  }

  // Visibility for testing
  public boolean kafkaLoggerReady() {
    if (kafkaLogger == null) {
      return false;
    }
    return kafkaLogger.ready();
  }
}
