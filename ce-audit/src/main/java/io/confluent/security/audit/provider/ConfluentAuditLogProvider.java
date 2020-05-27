/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.security.audit.provider;

import static io.confluent.security.audit.AuditLogConfig.AUDIT_CLOUD_EVENT_ENCODING_CONFIG;
import static io.confluent.security.audit.AuditLogConfig.toEventLoggerConfig;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.AUDIT_EVENT_ROUTER_CONFIG;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.AUDIT_LOGGER_ENABLE_CONFIG;
import static org.apache.kafka.common.config.internals.ConfluentConfigs.ENABLE_AUTHENTICATION_AUDIT_LOGS;

import io.cloudevents.CloudEvent;
import io.confluent.crn.ConfluentServerCrnAuthority;
import io.confluent.crn.CrnAuthorityConfig;
import io.confluent.crn.CrnSyntaxException;
import io.confluent.events.CloudEventUtils;
import io.confluent.events.EventLogger;
import io.confluent.events.ProtobufEvent;
import io.confluent.kafka.security.audit.event.ConfluentAuthenticationEvent;
import io.confluent.security.audit.AuditLogConfig;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.AuditLogUtils;
import io.confluent.security.audit.router.AuditLogRouter;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.ConfluentAuthorizationEvent;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.server.audit.AuditEvent;
import org.apache.kafka.server.audit.AuditEventType;
import org.apache.kafka.server.audit.AuditLogProvider;
import io.confluent.security.authorizer.utils.ThreadUtils;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.audit.AuthenticationEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfluentAuditLogProvider implements AuditLogProvider, ClusterResourceListener {

  public static final String AUTHORIZATION_MESSAGE_TYPE = "io.confluent.kafka.server/authorization";
  public static final String AUTHENTICATION_MESSAGE_TYPE = "io.confluent.kafka.server/authentication";

  protected static final Logger log = LoggerFactory.getLogger(ConfluentAuditLogProvider.class);
  private static final String FALLBACK_LOGGER = "io.confluent.security.audit.log.fallback";
  private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);
  // Fallback logger that is used if audit logging to Kafka topic fails or if events are not generated
  // TODO(sumit): Make sure this logger has sane defaults.
  protected final Logger fallbackLog = LoggerFactory.getLogger(FALLBACK_LOGGER);
  private UnaryOperator<AuditEvent> sanitizer;

  // The audit logger needs to be configured to produce to a destination Kafka cluster. If no
  // cluster is explicitly configured (which is the case in the default config, but might also
  // happen in other configurations), the audit logger produces to the local cluster over the
  // same listener that the inter-broker listener uses. Normally, this decision gets made in
  // start(), when the Provider is passed the interBrokerListenerConfigs. However, reconfiguration
  // can cause the config to be "reset" to a config that doesn't explicitly configure this.
  // To support that reconfiguration, we need to store the values we were given in start()
  private volatile Map<String, Object> originalInterBrokerListenerConfigs = new HashMap<>();

  // The Audit events feature has very strange requirements like dynamic config support for changing bootstrap servers
  // at runtime. They can have very high volume event streams for logging produce / consume audit events which
  // might need a dedicated cluster for storing audit events. They also have much more fluid topic requirements
  // (multiple topics depending on complex routing logic type) which can be configured dynamically through the UI.
  // Most other event types might not have these complicated needs. So, it might be better to create a dedicated
  // event logger for the audit log provider.
  private ConfiguredState configuredState;

  private ExecutorService initExecutor;

  private ConfluentServerCrnAuthority crnAuthority;

  private volatile boolean eventLoggerReady;
  private Scope scope;
  private boolean enableAuthenticationAuditLogs;

  @Override
  public void onUpdate(final ClusterResource clusterResource) {
    this.scope = Scope.kafkaClusterScope(clusterResource.clusterId());
  }

  private AuditLogMetrics auditLogMetrics;

  // The router is used in the logAuthorization() and can be reconfigured. It is best to update it atomically.

  // These should always be updated together
  private class ConfiguredState {

    final EventLogger logger;
    final AuditLogRouter router;
    final AuditLogConfig config;

    private ConfiguredState(EventLogger logger, AuditLogRouter router, AuditLogConfig config) {
      this.logger = logger;
      this.router = router;
      this.config = config;
    }
  }

  /**
   * The provider is configured and started during {@link #start(Map)} to get access to the
   * interbroker properties..
   */

  @Override
  public void configure(Map<String, ?> configs) {
    // Audit log config
    AuditLogConfig auditLogConfig = new AuditLogConfig(configs);

    // Abort if not enabled.
    if (!auditLogConfig.getBoolean(AUDIT_LOGGER_ENABLE_CONFIG)) {
      return;
    }

    enableAuthenticationAuditLogs = auditLogConfig.getBoolean(ENABLE_AUTHENTICATION_AUDIT_LOGS);

    this.configuredState = new ConfiguredState(
        new EventLogger(),
        new AuditLogRouter(
            auditLogConfig.routerJsonConfig(),
            auditLogConfig.getInt(AuditLogConfig.ROUTER_CACHE_ENTRIES_CONFIG)),
        auditLogConfig);

    CrnAuthorityConfig crnAuthorityConfig = new CrnAuthorityConfig(configs);
    this.crnAuthority = new ConfluentServerCrnAuthority();
    this.crnAuthority.configure(crnAuthorityConfig.values());

    this.eventLoggerReady = false;
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    Set<String> configs = new HashSet<>();
    // Only router config needs to be reconfigurable.
    configs.add(AUDIT_EVENT_ROUTER_CONFIG);
    return configs;
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    AuditLogConfig config = new AuditLogConfig(configs);
    // this actually creates the config, so it will throw a ConfigException if it's invalid
    config.routerJsonConfig();
  }

  private void updateConfiguredState(Map<String, Object> loggerConfig, AuditLogRouter router,
                                     AuditLogConfig config) {
    EventLogger oldLogger = configuredState != null ? configuredState.logger : null;
    EventLogger newLogger = new EventLogger();
    newLogger.configure(loggerConfig);
    configuredState = new ConfiguredState(newLogger, router, config);
    if (oldLogger != null) {
      // it is possible that in-flight events may be lost when this closes
      Utils.closeQuietly(oldLogger, "eventLogger");
    }
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    AuditLogConfig alc = new AuditLogConfig(configs);
    AuditLogRouterJsonConfig routerJsonConfig = alc.routerJsonConfig();
    Map<String, Object> newConfigs = new HashMap<>(configs);
    // If we don't have bootstrap servers specified, use the configs we saved at start
    if (alc.routerJsonConfig().bootstrapServers() == null) {
      newConfigs.putAll(originalInterBrokerListenerConfigs);
    }

    AuditLogRouter router =
        new AuditLogRouter(
            routerJsonConfig,
            alc.getInt(AuditLogConfig.ROUTER_CACHE_ENTRIES_CONFIG));

    // Merge the topics from the router config to the Kafka exporter config
    Map<String, Object> elc = toEventLoggerConfig(newConfigs);

    // Because the bootstrap servers may change, create a new event logger instance pointing
    // to the new bootstrap URL
    updateConfiguredState(elc, router, alc);
  }

  @Override
  public CompletionStage<Void> start(Map<String, ?> interBrokerListenerConfigs) {
    initExecutor = Executors.
        newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("audit-init-%d", true));
    // save these, in case we're reconfigured without bootstrap servers
    this.originalInterBrokerListenerConfigs = new HashMap<>(interBrokerListenerConfigs);
    CompletableFuture<Void> future = new CompletableFuture<>();
    initExecutor.submit(() -> {
      try {
        // start with the configs to connect to the inter broker listener
        Map<String, Object> config = new HashMap<>(interBrokerListenerConfigs);
        // add the values for AuditLogConfig settings
        config.putAll(configuredState.config.values());

        updateConfiguredState(toEventLoggerConfig(config),
            configuredState.router, configuredState.config);
        this.eventLoggerReady = true;
        future.complete(null);
      } catch (Throwable e) {
        log.error("Audit log provider could not be started", e);
        future.completeExceptionally(e);
      }
    });
    return future.whenComplete((unused, e) -> initExecutor.shutdownNow());
  }

  @Override
  public void logEvent(final AuditEvent auditEvent) {
    if (auditEvent.type() == AuditEventType.AUTHORIZATION) {
      logAuthorization((ConfluentAuthorizationEvent) auditEvent);
    } else if (auditEvent.type() == AuditEventType.AUTHENTICATION) {
      logAuthentication((AuthenticationEvent) auditEvent);
    } else {
      log.error("Unknown event received {}", auditEvent);
    }
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    // If we can determine that none of the log destinations are in this cluster, we can return
    // false. Returning true for now as the safe option.
    return true;
  }

  @Override
  public boolean providerConfigured(Map<String, ?> configs) {
    AuditLogConfig cfg = new AuditLogConfig(configs);
    return cfg.getBoolean(AUDIT_LOGGER_ENABLE_CONFIG);
  }

  @Override
  public void setSanitizer(UnaryOperator<AuditEvent> sanitizer) {
    this.sanitizer = sanitizer;
  }

  @Override
  public void setMetrics(Metrics metrics) {
    auditLogMetrics = new AuditLogMetrics(metrics);
  }

  private void logAuthorization(ConfluentAuthorizationEvent authorizationEvent) {
    try {
      AuditLogEntry auditLogEntry = AuditLogUtils.authorizationEvent(authorizationEvent, crnAuthority);

      // use the config and event logger from a particular point in time
      ConfiguredState state = this.configuredState;

      // Figure out the topic to route.
      Optional<String> route = state.router.topic(auditLogEntry);
      if (isSuppressed(route)) {
        return;
      }

      // at this point we've decided that we intend to log, so we calculate the content
      if (sanitizer != null) {
        authorizationEvent = (ConfluentAuthorizationEvent) sanitizer.apply(authorizationEvent);
        if (authorizationEvent == null) {
          return;
        }

        // need to recalculate these with the transformed authZEvent data
        auditLogEntry = AuditLogUtils.authorizationEvent(authorizationEvent, crnAuthority);
      }

      ProtobufEvent.Builder eventBuilder = eventBuilder(auditLogEntry, state, authorizationEvent, AUTHORIZATION_MESSAGE_TYPE);
      logEventToRoute(state, route, eventBuilder, !eventLoggerReady || !shouldSendToKafka(authorizationEvent));
    } catch (CrnSyntaxException e) {
      log.error("Couldn't create cloud event due to internally generated CRN syntax problem", e);
    }
  }

  private boolean isSuppressed(Optional<String> route) {
    return route.isPresent() && route.get().equalsIgnoreCase(AuditLogRouter.SUPPRESSED);
  }

  private void logEventToRoute(final ConfiguredState state, final Optional<String> route,
                               final ProtobufEvent.Builder eventBuilder, final boolean fallback) {
    if (fallback) {
      fallbackLog.info(CloudEventUtils.toJsonString(eventBuilder.build()));
      auditLogMetrics.recordFallbackAuditlogMetrics();
      return;
    }

    if (route.isPresent()) {
      eventBuilder.setRoute(route.get());
    } else {
      fallbackLog.error("Empty topic for {}", CloudEventUtils.toJsonString(eventBuilder.build()));
      return;
    }

    // Make sure Kafka exporter is ready to receive events.
    CloudEvent event = eventBuilder.build();
    boolean routeReady = state.logger.ready(event);
    if (routeReady) {
      state.logger.log(event);
      auditLogMetrics.recordNormalAuditlogMetrics();
    } else {
      fallbackLog.info(CloudEventUtils.toJsonString(event));
      auditLogMetrics.recordFallbackAuditlogMetrics();
    }
  }

  private ProtobufEvent.Builder eventBuilder(final AuditLogEntry entry, final ConfiguredState state,
                                             final AuditEvent auditEvent,
                                             final String messageType) {
    return ProtobufEvent.newBuilder()
        .setId(auditEvent.uuid().toString())
        .setTime(auditEvent.timestamp().atZone(ZoneOffset.UTC))
        .setData(entry)
        .setSource(entry.getServiceName())
        .setSubject(entry.getResourceName())
        .setType(messageType)
        .setEncoding(state.config.getString(AUDIT_CLOUD_EVENT_ENCODING_CONFIG));
  }

  /**
   * Should authorization event be sent to Kafka ?
   */
  private boolean shouldSendToKafka(final ConfluentAuthorizationEvent authorizationEvent) {
    switch (authorizationEvent.authorizeResult()) {
      case ALLOWED:
        return authorizationEvent.action().logIfAllowed();
      case DENIED:
        return authorizationEvent.action().logIfDenied();
      default:
        return true;
    }
  }

  private void logAuthentication(AuthenticationEvent auditEvent) {
    // check enable authentication flag
    if (!enableAuthenticationAuditLogs) return;

    try {
      ConfluentAuthenticationEvent authenticationEvent = new ConfluentAuthenticationEvent(auditEvent, scope);
      AuditLogEntry entry = AuditLogUtils.authenticationEvent(authenticationEvent, crnAuthority);

      // use the config and event logger from a particular point in time
      ConfiguredState state = this.configuredState;

      // Figure out the topic to route.
      Optional<String> route = state.router.topic(entry);
      if (isSuppressed(route)) {
        return;
      }

      // at this point we've decided that we intend to log, so we calculate the content
      if (sanitizer != null) {
        authenticationEvent = (ConfluentAuthenticationEvent) sanitizer.apply(authenticationEvent);
        if (auditEvent == null) {
          return;
        }

        entry = AuditLogUtils.authenticationEvent(authenticationEvent, crnAuthority);
      }

      ProtobufEvent.Builder eventBuilder = eventBuilder(entry, state, auditEvent, AUTHENTICATION_MESSAGE_TYPE);
      logEventToRoute(state, route, eventBuilder, !eventLoggerReady);
    } catch (Exception e) {
      log.error("Error occurred while handling authentication event : {}", auditEvent, e);
    }
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
    Utils.closeQuietly(configuredState.logger, "eventLogger");
  }

  // Visibility for testing
  public ExecutorService initExecutor() {
    return initExecutor;
  }

  // Visibility for testing
  public EventLogger getEventLogger() {
    return configuredState.logger;
  }

  // Visibility for testing
  public boolean isEventLoggerReady() {
    return eventLoggerReady;
  }

  // Visibility for testing
  protected Metrics metrics() {
    return auditLogMetrics.metrics();
  }

  // Visibility for testing
  protected void setupMetrics(Time time) {
    auditLogMetrics = new AuditLogMetrics(time);
  }

  // Visibility for testing
  protected Time metricsTime() {
    return auditLogMetrics.metricsTime();
  }

  class AuditLogMetrics {
    public static final String GROUP_NAME = "confluent-audit-metrics";
    public static final String AUDIT_LOG_RATE_MINUTE = "audit-log-rate-per-minute";
    public static final String AUDIT_LOG_FALLBACK_RATE_MINUTE = "audit-log-fallback-rate-per-minute";
    private static final String AUDIT_LOG_NORMAL_SENSOR = "audit-log-normal";
    private static final String AUDIT_LOG_FALLBACK_SENSOR = "audit-log-fallback";
    private Sensor normalAuditSensor = null;
    private Sensor fallbackAuditSensor = null;
    private Time time;
    private Metrics metrics;

    AuditLogMetrics(Metrics metrics) {
      this.metrics = metrics;
      setupMetrics();
    }

    // Visibility for testing
    AuditLogMetrics(Time time) {
      this.time = time;
      this.metrics = new Metrics(time);
      setupMetrics();
    }

    void recordNormalAuditlogMetrics() {
      normalAuditSensor.record();
    }

    void recordFallbackAuditlogMetrics() {
      fallbackAuditSensor.record();
    }

    Metrics metrics() {
      return metrics;
    }

    Time metricsTime() {
      return time;
    }

    void setupMetrics() {
      normalAuditSensor = metrics.sensor(AUDIT_LOG_NORMAL_SENSOR);
      normalAuditSensor.add(metrics.metricName(AUDIT_LOG_RATE_MINUTE, GROUP_NAME,
              "The number of audit log per minute"), new Rate(TimeUnit.MINUTES, new WindowedCount()));
      fallbackAuditSensor = metrics.sensor(AUDIT_LOG_FALLBACK_SENSOR);
      fallbackAuditSensor.add(metrics.metricName(AUDIT_LOG_FALLBACK_RATE_MINUTE, GROUP_NAME,
              "The number of audit log fallback per minute"), new Rate(TimeUnit.MINUTES, new WindowedCount()));
    }
  }
}
