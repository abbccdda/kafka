/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.security.audit.provider;

import static io.confluent.security.audit.AuditLogConfig.AUDIT_CLOUD_EVENT_ENCODING_CONFIG;
import static io.confluent.security.audit.AuditLogConfig.ROUTER_CONFIG;
import static io.confluent.security.audit.AuditLogConfig.toEventLoggerConfig;

import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.crn.ConfluentServerCrnAuthority;
import io.confluent.crn.CrnAuthorityConfig;
import io.confluent.crn.CrnSyntaxException;
import io.confluent.events.CloudEventUtils;
import io.confluent.events.EventLogger;
import io.confluent.events.ProtobufEvent;
import io.confluent.security.audit.AuditLogConfig;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.AuditLogUtils;
import io.confluent.security.audit.router.AuditLogRouter;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.AuthorizePolicy;
import io.confluent.security.authorizer.AuthorizeResult;
import io.confluent.security.authorizer.RequestContext;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.AuditLogProvider;
import io.confluent.security.authorizer.utils.ThreadUtils;
import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfluentAuditLogProvider implements AuditLogProvider, ClusterResourceListener {

  public static final String AUTHORIZATION_MESSAGE_TYPE = "io.confluent.kafka.server/authorization";
  protected static final Logger log = LoggerFactory.getLogger(ConfluentAuditLogProvider.class);
  private static final String FALLBACK_LOGGER = "io.confluent.security.audit.log.fallback";
  private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);
  // Fallback logger that is used if audit logging to Kafka topic fails or if events are not generated
  // TODO(sumit): Make sure this logger has sane defaults.
  protected final Logger fallbackLog = LoggerFactory.getLogger(FALLBACK_LOGGER);

  // The Audit events feature has very strange requirements like dynamic config support for changing bootstrap servers
  // at runtime. They can have very high volume event streams for logging produce / consume audit events which
  // might need a dedicated cluster for storing audit events. They also have much more fluid topic requirements
  // (multiple topics depending on complex routing logic type) which can be configured dynamically through the UI.
  // Most other event types might not have these complicated needs. So, it might be better to create a dedicated
  // event logger for the audit log provider.
  private EventLogger eventLogger;

  private ExecutorService initExecutor;

  private ConfluentServerCrnAuthority crnAuthority;

  private volatile boolean eventLoggerReady;
  private String clusterId;

  private Scope scope;
  // The router is used in the logAuthorization() and can be reconfigured. It is best to update it atomically.
  private AtomicReference<AuditLogRouter> router;
  private AuditLogConfig auditLogConfig;

  @Override
  public void onUpdate(ClusterResource clusterResource) {
    this.clusterId = clusterResource.clusterId();
    this.scope = Scope.kafkaClusterScope(clusterId);
    this.scope.validate(false);

  }

  /**
   * The provider is configured and started during {@link #start(Map)} to get access to the
   * interbroker properties..
   */

  @Override
  public void configure(Map<String, ?> configs) {
    // Audit log config
    auditLogConfig = new AuditLogConfig(configs);

    // Abort if not enabled.
    if (!auditLogConfig.getBoolean(AuditLogConfig.AUDIT_LOGGER_ENABLED_CONFIG)) {
      return;
    }

    // Create event logger
    eventLogger = new EventLogger();

    router = new AtomicReference<>(
        new AuditLogRouter(
            auditLogConfig.routerJsonConfig(),
            auditLogConfig.getInt(AuditLogConfig.ROUTER_CACHE_ENTRIES_CONFIG)));

    CrnAuthorityConfig crnAuthorityConfig = new CrnAuthorityConfig(configs);
    this.crnAuthority = new ConfluentServerCrnAuthority();
    this.crnAuthority.configure(crnAuthorityConfig.values());

    this.eventLoggerReady = false;
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    Set<String> configs = new HashSet<>();
    // Only router config needs to be reconfigurable.
    configs.add(ROUTER_CONFIG);
    return configs;
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    AuditLogConfig config = new AuditLogConfig(configs);
    try {
      AuditLogRouterJsonConfig.load(config.getString(ROUTER_CONFIG));
    } catch (IllegalArgumentException | IOException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    this.auditLogConfig = new AuditLogConfig(configs);
    this.router = new AtomicReference<>(
        new AuditLogRouter(
            auditLogConfig.routerJsonConfig(),
            auditLogConfig.getInt(AuditLogConfig.ROUTER_CACHE_ENTRIES_CONFIG)));

    // Merge the topics from the router config to the Kafka exporter config
    Map<String, Object> elConfig = toEventLoggerConfig(configs);

    // If bootstrap servers change, create a new event logger instance pointing to the new
    // bootstrap URL otherwise, just call reconfigure which add the new topics to be managed.
    // This will not alter the topics if retention_ms is updated.If this is desired, then the
    // customer will need to update them using other topic management tools.
    if (auditLogConfig.routerJsonConfig().destinations.bootstrapServers !=
        auditLogConfig.routerJsonConfig().destinations.bootstrapServers) {
      try {
        eventLogger.close();
      } catch (Exception e) {
        log.error("error while closing the event exporter during reconfiguration", e);
      }
      eventLogger = new EventLogger();
      eventLogger.configure(elConfig);
    } else {
      eventLogger.reconfigure(elConfig);
    }
  }

  @Override
  public CompletionStage<Void> start(Map<String, ?> interBrokerListenerConfigs) {
    initExecutor = Executors.
        newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("audit-init-%d", true));

    CompletableFuture<Void> future = new CompletableFuture<>();
    initExecutor.submit(() -> {
      try {
        eventLogger.configure(toEventLoggerConfig(interBrokerListenerConfigs));
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
  public boolean providerConfigured(Map<String, ?> configs) {
    AuditLogConfig cfg = new AuditLogConfig(configs);
    return cfg.getBoolean(AuditLogConfig.AUDIT_LOGGER_ENABLED_CONFIG);
  }

  @Override
  public void logAuthorization(RequestContext requestContext,
      Action action,
      AuthorizeResult authorizeResult,
      AuthorizePolicy authorizePolicy) {

    // Should this event be sent to Kafka ?
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

    try {
      String source = crnAuthority.canonicalCrn(scope).toString();
      String subject = crnAuthority.canonicalCrn(action.scope(), action.resourcePattern())
          .toString();

      AuditLogEntry entry = AuditLogUtils
          .authorizationEvent(source, subject, requestContext, action, authorizeResult,
              authorizePolicy);

      ProtobufEvent.Builder eventBuilder = ProtobufEvent.newBuilder()
          .setData(entry)
          .setSource(source)
          .setSubject(subject)
          .setType(AUTHORIZATION_MESSAGE_TYPE)
          .setEncoding(auditLogConfig.getString(AUDIT_CLOUD_EVENT_ENCODING_CONFIG));

      // Filter out events with the logging principal to prevent infinite loops.
      boolean dropEvent = hasEventLogPrincipal(entry, auditLogConfig);

      if (!eventLoggerReady || !generateEvent || dropEvent) {
        fallbackLog.info(CloudEventUtils.toJsonString(eventBuilder.build()));
        return;
      }

      // Figure out the topic.
      Optional<String> route = router.get()
          .topic((CloudEvent<AttributesImpl, AuditLogEntry>) eventBuilder.build());

      if (route.isPresent()) {
        if (route.get().equalsIgnoreCase(AuditLogRouter.SUPPRESSED)) {
          return;
        }
        eventBuilder.setRoute(route.get());
      } else {
        fallbackLog.error("Empty topic for {}", CloudEventUtils.toJsonString(eventBuilder.build()));
        return;
      }

      // Make sure Kafka exporter is ready to receive events.
      CloudEvent event = eventBuilder.build();
      boolean routeReady = eventLogger.ready(event);
      if (routeReady) {
        eventLogger.log(event);
      } else {
        fallbackLog.info(CloudEventUtils.toJsonString(event));
      }

    } catch (CrnSyntaxException e) {
      log.error("Couldn't create cloud event due to internally generated CRN syntax problem", e);
    }
  }

  // suppress all events for the principal that is doing this logging to make sure we don't loop infinitely
  private boolean hasEventLogPrincipal(AuditLogEntry auditLogEntry, AuditLogConfig eventLogConfig) {
    KafkaPrincipal eventLogPrincipal = eventLogConfig.eventLogPrincipal();
    KafkaPrincipal eventPrincipal = SecurityUtils
        .parseKafkaPrincipal(auditLogEntry.getAuthenticationInfo().getPrincipal());
    return eventLogPrincipal.equals(eventPrincipal);
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
    Utils.closeQuietly(eventLogger, "eventLogger");
  }


  // Visibility for testing
  public ExecutorService initExecutor() {
    return initExecutor;
  }

  // Visibility for testing
  public EventLogger getEventLogger() {
    return eventLogger;
  }

  // Visibility for testing
  public Scope getScope() {
    return scope;
  }

  // Visibility for testing
  public boolean isEventLoggerReady() {
    return eventLoggerReady;
  }
}
