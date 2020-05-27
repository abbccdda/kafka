// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import io.confluent.security.authorizer.AuthorizePolicy.PolicyType;
import io.confluent.security.authorizer.AuthorizePolicy.SuperUser;
import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.Auditable;
import io.confluent.security.authorizer.provider.ConfluentAuthorizationEvent;
import io.confluent.security.authorizer.provider.DefaultAuditLogProvider;
import io.confluent.security.authorizer.provider.GroupProvider;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.authorizer.provider.AuthorizeRule;
import io.confluent.security.authorizer.provider.MetadataProvider;
import io.confluent.security.authorizer.provider.Provider;
import io.confluent.security.authorizer.provider.ProviderFailedException;
import io.confluent.security.authorizer.utils.ThreadUtils;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.authorizer.internals.ConfluentAuthorizerServerInfo;
import org.apache.kafka.server.audit.AuditEvent;
import org.apache.kafka.server.audit.AuditLogProvider;
import org.apache.kafka.server.audit.NoOpAuditLogProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cross-component embedded authorizer that implements common authorization logic. This
 * authorizer loads configured providers and uses them to perform authorization.
 */
public class EmbeddedAuthorizer implements Authorizer {

  protected static final Logger log = LoggerFactory.getLogger("kafka.authorizer.logger");

  protected final Set<Provider> providersCreated;
  private GroupProvider groupProvider;
  private List<AccessRuleProvider> accessRuleProviders;
  private AuditLogProvider auditLogProvider;
  protected ConfluentAuthorizerConfig authorizerConfig;
  private MetadataProvider metadataProvider;
  private boolean allowEveryoneIfNoAcl;
  private Set<KafkaPrincipal> superUsers;
  protected Set<KafkaPrincipal> brokerUsers;
  protected String interBrokerListener;
  private Duration initTimeout;
  private volatile boolean ready;
  private volatile String clusterId;
  private volatile Scope scope;
  private AuthorizerMetrics authorizerMetrics;


  public EmbeddedAuthorizer() {
    this.providersCreated = new HashSet<>();
    this.superUsers = Collections.emptySet();
    this.brokerUsers = Collections.emptySet();
    this.scope = Scope.ROOT_SCOPE; // Scope is only required for broker authorizers using RBAC
  }

  @Override
  public void configure(Map<String, ?> configs) {
    authorizerConfig = new ConfluentAuthorizerConfig(configs);
    allowEveryoneIfNoAcl = authorizerConfig.allowEveryoneIfNoAcl;
    superUsers = authorizerConfig.superUsers;
    brokerUsers = authorizerConfig.brokerUsers;
  }

  public void configureServerInfo(ConfluentAuthorizerServerInfo serverInfo) {
    this.clusterId = serverInfo.clusterResource().clusterId();
    log.debug("Configuring scope for Kafka cluster with cluster id {}", clusterId);
    this.scope = Scope.kafkaClusterScope(clusterId);
    this.interBrokerListener = serverInfo.interBrokerEndpoint().listenerName().get();
    this.authorizerMetrics = new AuthorizerMetrics(serverInfo.metrics());

    ConfluentAuthorizerConfig.Providers providers =
        authorizerConfig.createProviders(clusterId, serverInfo.auditLogProvider());
    providersCreated.addAll(providers.accessRuleProviders);
    if (providers.groupProvider != null)
      providersCreated.add(providers.groupProvider);
    if (providers.metadataProvider != null)
      providersCreated.add(providers.metadataProvider);

    providersCreated.stream().filter(provider -> provider instanceof Auditable)
        .forEach(provider -> ((Auditable) provider).auditLogProvider(serverInfo.auditLogProvider()));

    configureProviders(providers.accessRuleProviders,
        providers.groupProvider,
        providers.metadataProvider,
        serverInfo.auditLogProvider());
  }

  @Override
  public List<AuthorizeResult> authorize(RequestContext requestContext, List<Action> actions) {
    return  actions.stream()
        .map(action -> authorize(requestContext, action))
        .collect(Collectors.toList());
  }

  // Visibility for testing
  public GroupProvider groupProvider() {
    return groupProvider;
  }

  // Visibility for testing
  public AccessRuleProvider accessRuleProvider(String providerName) {
    Optional<AccessRuleProvider> provider = accessRuleProviders.stream()
        .filter(p -> p.providerName().equals(providerName))
        .findFirst();
    if (provider.isPresent())
      return provider.get();
    else
      throw new IllegalArgumentException("Access rule provider not found: " + providerName);
  }

  // Visibility for testing
  public MetadataProvider metadataProvider() {
    return metadataProvider;
  }

  protected List<AccessRuleProvider> accessRuleProviders() {
    return accessRuleProviders;
  }

  // Visibility for testing
  public AuditLogProvider auditLogProvider() {
    return auditLogProvider;
  }

  public CompletableFuture<Void> start(ConfluentAuthorizerServerInfo serverInfo,
                                       Map<String, ?> interBrokerListenerConfigs, Runnable initTask) {
    initTimeout = authorizerConfig.initTimeout;
    /* Normal route configureServerInfo should be called by caller first so that authorizerMetrics got initialized there.
     * For some unit tests, a caller doesn't call configureServerInfo and directly call start. So I create
     * authorizerMetrics on two places. First it is created in configureServerInfo, second it is created in start
     * method after checking it is not null.
     */
    if (authorizerMetrics == null)
      authorizerMetrics = new AuthorizerMetrics(serverInfo.metrics());

    Set<Provider> allProviders = new HashSet<>(); // Use a set to remove duplicates
    if (groupProvider != null)
      allProviders.add(groupProvider);
    allProviders.addAll(accessRuleProviders);

    if (metadataProvider != null) {
      allProviders.add(metadataProvider);
    }

    List<CompletableFuture<Void>> futures = allProviders.stream()
        .map(provider -> provider.start(serverInfo, interBrokerListenerConfigs))
        .map(CompletionStage::toCompletableFuture)
        .collect(Collectors.toList());
    CompletableFuture[] futureArray = futures.toArray(new CompletableFuture[futures.size()]);
    CompletableFuture<Void> readyFuture = CompletableFuture.allOf(futureArray)
        .thenAccept(unused -> this.ready = true)
        //server startup will fail if any error/invalid mds configs during migration/init task
        .thenRunAsync(initTask)
        .thenAccept(unused -> auditLogProvider.start(interBrokerListenerConfigs));
    CompletableFuture<Void> future = futureOrTimeout(readyFuture, initTimeout);

    // For clusters that are not hosting the metadata topic, we can safely wait for the
    // future to complete before any listeners are started. For brokers on the cluster
    // hosting the metadata topic, we will wait later after starting the inter-broker
    // listener, to enable metadata to be loaded into cache prior to accepting connections
    // on other listeners.
    boolean usesMetadataFromThisKafkaCluster =
        (groupProvider != null && groupProvider.usesMetadataFromThisKafkaCluster()) ||
        (metadataProvider != null && metadataProvider.usesMetadataFromThisKafkaCluster()) ||
         accessRuleProviders.stream().anyMatch(AccessRuleProvider::usesMetadataFromThisKafkaCluster) ||
            auditLogProvider.usesMetadataFromThisKafkaCluster();
    if (!usesMetadataFromThisKafkaCluster)
      future.join();
    return future;
  }

  protected void configureProviders(List<AccessRuleProvider> accessRuleProviders,
                                    GroupProvider groupProvider,
                                    MetadataProvider metadataProvider,
                                    AuditLogProvider auditLogProvider) {
    this.accessRuleProviders = accessRuleProviders;
    this.groupProvider = groupProvider;
    this.metadataProvider = metadataProvider;
    this.auditLogProvider = (auditLogProvider == null || auditLogProvider == NoOpAuditLogProvider.INSTANCE) ?
        new DefaultAuditLogProvider() : auditLogProvider;
  }

  protected boolean ready() {
    return ready;
  }

  protected boolean isSuperUser(KafkaPrincipal sessionPrincipal,
                                KafkaPrincipal userOrGroupPrincipal,
                                Action action) {
    return superUsers.contains(userOrGroupPrincipal);
  }

  private AuthorizeResult authorize(RequestContext requestContext, Action action) {
    try {
      AuthorizePolicy authorizePolicy;
      KafkaPrincipal sessionPrincipal = requestContext.principal();
      String host = requestContext.clientAddress().getHostAddress();
      KafkaPrincipal userPrincipal = userPrincipal(sessionPrincipal);

      if (isSuperUser(sessionPrincipal, userPrincipal, action)) {
        log.debug("principal = {} is a super user, allowing operation without checking any providers.", userPrincipal);
        authorizePolicy = new SuperUser(PolicyType.SUPER_USER, userPrincipal);
      } else {
        Set<KafkaPrincipal> groupPrincipals = groupProvider.groups(sessionPrincipal);
        Optional<KafkaPrincipal> superGroup = groupPrincipals.stream()
            .filter(group -> isSuperUser(sessionPrincipal, group, action)).findFirst();
        if (superGroup.isPresent()) {
          log.debug("principal = {} belongs to super group {}, allowing operation without checking acls.",
              userPrincipal, superGroup.get());
          authorizePolicy = new SuperUser(PolicyType.SUPER_GROUP, superGroup.get());
        } else {
          authorizePolicy = authorize(sessionPrincipal, groupPrincipals, host, action);
        }
      }

      AuthorizeResult authorizeResult = authorizePolicy.policyType().accessGranted() ? AuthorizeResult.ALLOWED : AuthorizeResult.DENIED;
      try {
        logAuditMessage(scope, requestContext, action, authorizeResult, authorizePolicy);
      } catch (Exception e) {
        log.error("Failed to log Audit message.\n  scope: {}\n  context: {}\n  principal: {}\n"
                + "  action: {}\n  result: {}\n  policy: {}",
            scope, requestContext, requestContext.principal(),
            action, authorizeResult, authorizePolicy,
            e);
      }

      // Record authorizer metrics
      authorizerMetrics.recordAuthorizerMetrics(authorizeResult);

      return authorizeResult;

    } catch (InvalidScopeException e) {
      log.error("Authorizer failed with unknown scope: {}", action.scope(), e);
      return AuthorizeResult.UNKNOWN_SCOPE;
    } catch (ProviderFailedException e) {
      log.error("Authorization provider has failed", e);
      return AuthorizeResult.AUTHORIZER_FAILED;
    } catch (Throwable t) {
      log.error("Authorization failed with unexpected exception", t);
      return AuthorizeResult.UNKNOWN_ERROR;
    }
  }

  private AuthorizePolicy authorize(KafkaPrincipal sessionPrincipal,
      Set<KafkaPrincipal> groupPrincipals,
      String host,
      Action action) {

    Scope scope = action.scope();
    if (accessRuleProviders.stream()
        .anyMatch(p -> p.isSuperUser(sessionPrincipal, scope))) {
      return new SuperUser(PolicyType.SUPER_USER, sessionPrincipal);
    }
    Optional<KafkaPrincipal> superGroup = groupPrincipals.stream()
        .filter(principal -> accessRuleProviders.stream().anyMatch(p -> p.isSuperUser(principal, scope)))
        .findAny();
    if (superGroup.isPresent()) {
      return new SuperUser(PolicyType.SUPER_GROUP, superGroup.get());
    }

    ResourcePattern resource = action.resourcePattern();
    Operation operation = action.operation();
    AuthorizeRule authorizeRule = new AuthorizeRule();
    accessRuleProviders.stream()
        .filter(AccessRuleProvider::mayDeny)
        .forEach(p -> authorizeRule.add(p.findRule(sessionPrincipal, groupPrincipals, host, action)));

    // Check if there is any Deny acl match that would disallow this operation.
    Optional<AuthorizePolicy> authorizePolicy;
    if ((authorizePolicy = authorizePolicy(operation, resource, host, authorizeRule)).isPresent())
      return authorizePolicy.get();

    accessRuleProviders.stream()
        .filter(p -> !p.mayDeny())
        .forEach(p -> authorizeRule.add(p.findRule(sessionPrincipal, groupPrincipals, host, action)));

    // Check if there are any Allow ACLs which would allow this operation.
    if ((authorizePolicy = authorizePolicy(operation, resource, host, authorizeRule)).isPresent())
      return authorizePolicy.get();

    return authorizePolicy.orElse(authorizePolicyWithNoMatchingRule(resource, authorizeRule));
  }

  @Override
  public void close() {
    log.debug("Closing embedded authorizer");
    AtomicReference<Throwable> firstException = new AtomicReference<>();
    providersCreated.forEach(provider ->
        Utils.closeQuietly(provider, provider.providerName(), firstException));
    Throwable exception = firstException.getAndSet(null);
    // We don't want to prevent clean broker shutdown if providers are not gracefully closed.
    if (exception != null)
      log.error("Failed to close authorizer cleanly", exception);
  }

  protected Scope scope() {
    return scope;
  }

  /* Confluent provider create the EmbeddedAuthorizer without starting it, so need setup Kafka metrics */
  protected void setupAuthorizerMetrics(Metrics metrics) {
    authorizerMetrics = new AuthorizerMetrics(metrics);
  }

  private Optional<AuthorizePolicy> authorizePolicy(Operation op,
                                                    ResourcePattern resource,
                                                    String host,
                                                    AuthorizeRule authorizeRule) {
    Optional<AccessRule> ruleOpt = authorizeRule.denyRule().isPresent() ?
        authorizeRule.denyRule() : authorizeRule.allowRule();
    ruleOpt.ifPresent(rule -> {
      log.debug("operation = {} on resource = {} from host = {} is {} based on policy = {}",
            op, resource, host, rule.permissionType(), rule);
    });
    return ruleOpt.map(Function.identity());
  }

  private AuthorizePolicy authorizePolicyWithNoMatchingRule(ResourcePattern resource, AuthorizeRule authorizeRule) {
    if (authorizeRule.noResourceAcls()) {
      log.debug("No acl found for resource {}, authorized = {}", resource, allowEveryoneIfNoAcl);
      return allowEveryoneIfNoAcl ? AuthorizePolicy.ALLOW_ON_NO_RULE : AuthorizePolicy.DENY_ON_NO_RULE;
    } else {
      return AuthorizePolicy.NO_MATCHING_RULE;
    }
  }

  private KafkaPrincipal userPrincipal(KafkaPrincipal sessionPrincipal) {
    return sessionPrincipal.getClass() != KafkaPrincipal.class
        ? new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName())
        : sessionPrincipal;
  }

  protected void logAuditMessage(Scope sourceScope,
      RequestContext requestContext,
      Action action,
      AuthorizeResult authorizeResult,
      AuthorizePolicy authorizePolicy) {
    AuditEvent auditEvent = new ConfluentAuthorizationEvent(sourceScope, requestContext, action,
        authorizeResult, authorizePolicy);
    auditLogProvider.logEvent(auditEvent);
  }

  // Visibility for testing
  CompletableFuture<Void> futureOrTimeout(CompletableFuture<Void> readyFuture, Duration timeout) {
    if (readyFuture.isDone())
      return readyFuture;
    CompletableFuture<Void> timeoutFuture = new CompletableFuture<>();
    ScheduledExecutorService executor =
        Executors.newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("authorizer-%d", true));
    executor.schedule(() -> {
      TimeoutException e = new TimeoutException("Authorizer did not start up within timeout " +
          timeout.toMillis() + " ms.");
      timeoutFuture.completeExceptionally(e);
    }, timeout.toMillis(), TimeUnit.MILLISECONDS);
    return CompletableFuture.anyOf(readyFuture, timeoutFuture)
        .thenApply(unused -> (Void) null)
        .whenComplete((unused, e) -> executor.shutdownNow());
  }

  // Visibility for testing
  protected Metrics metrics() {
    return authorizerMetrics.metrics();
  }

  // Visible for testing only
  void setupAuthorizerMetrics(Time time) {
    authorizerMetrics = new AuthorizerMetrics(time);
  }

  // Visibility for testing
  protected Time metricsTime() {
    return authorizerMetrics.metricsTime();
  }

  class AuthorizerMetrics {
    public static final String GROUP_NAME = "confluent-authorizer-metrics";
    public static final String AUTHORIZATION_REQUEST_RATE_MINUTE = "authorization-request-rate-per-minute";
    public static final String AUTHORIZATION_ALLOWED_RATE_MINUTE = "authorization-allowed-rate-per-minute";
    public static final String AUTHORIZATION_DENIED_RATE_MINUTE = "authorization-denied-rate-per-minute";
    private static final String AUTHORIZER_AUTHORIZATION_ALLOWED_SENSOR = "authorizer-authorization-allowed";
    private static final String AUTHORIZER_AUTHORIZATION_DENIED_SENSOR = "authorizer-authorization-denied";
    private static final String AUTHORIZER_AUTHORIZATION_REQUEST_SENSOR = "authorizer-authorization-request";
    private Time time = null;
    private Metrics metrics = null;
    private Sensor authorizationAllowedSensor = null;
    private Sensor authorizationDeniedSensor = null;
    private Sensor authorizationRequestSensor = null;

    AuthorizerMetrics(Metrics metrics) {
      this.metrics = metrics;
      setupMetrics();
    }

    // Visibility for testing
    AuthorizerMetrics(Time time) {
      this.time = time;
      this.metrics = new Metrics(time);
      setupMetrics();
    }

    void recordAuthorizerMetrics(AuthorizeResult result) {
      authorizationRequestSensor.record();
      if (result == AuthorizeResult.ALLOWED) {
        authorizationAllowedSensor.record();
      } else {
        authorizationDeniedSensor.record();
      }
    }

    Metrics metrics() {
      return metrics;
    }

    Time metricsTime() {
      return time;
    }

    void setupMetrics() {
      authorizationAllowedSensor = metrics.sensor(AUTHORIZER_AUTHORIZATION_ALLOWED_SENSOR);
      authorizationAllowedSensor.add(metrics.metricName(AUTHORIZATION_ALLOWED_RATE_MINUTE, GROUP_NAME,
              "The number of authorization allowed per minute"), new Rate(TimeUnit.MINUTES, new WindowedCount()));

      authorizationDeniedSensor = metrics.sensor(AUTHORIZER_AUTHORIZATION_DENIED_SENSOR);
      authorizationDeniedSensor.add(metrics.metricName(AUTHORIZATION_DENIED_RATE_MINUTE, GROUP_NAME,
              "The number of authorization denied per minute"), new Rate(TimeUnit.MINUTES, new WindowedCount()));

      authorizationRequestSensor = metrics.sensor(AUTHORIZER_AUTHORIZATION_REQUEST_SENSOR);
      authorizationRequestSensor.add(metrics.metricName(AUTHORIZATION_REQUEST_RATE_MINUTE, GROUP_NAME,
              "The number of authorization request per minute"), new Rate(TimeUnit.MINUTES, new WindowedCount()));
    }
  }
}
