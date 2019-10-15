// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import io.confluent.security.authorizer.AuthorizePolicy.PolicyType;
import io.confluent.security.authorizer.AuthorizePolicy.SuperUser;
import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.AuditLogProvider;
import io.confluent.security.authorizer.provider.DefaultAuditLogProvider;
import io.confluent.security.authorizer.provider.MetadataProvider;
import io.confluent.security.authorizer.provider.Provider;
import io.confluent.security.authorizer.provider.ProviderFailedException;
import io.confluent.security.authorizer.provider.GroupProvider;
import io.confluent.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.authorizer.utils.ThreadUtils;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cross-component embedded authorizer that implements common authorization logic. This
 * authorizer loads configured providers and uses them to perform authorization.
 */
public class EmbeddedAuthorizer implements Authorizer {

  protected static final Logger log = LoggerFactory.getLogger("kafka.authorizer.logger");

  private static final Map<Operation, Collection<Operation>> IMPLICIT_ALLOWED_OPS;

  private final Set<Provider> providersCreated;
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
  private boolean usesMetadataFromThisKafkaCluster;
  private volatile boolean ready;
  private volatile String clusterId;
  private volatile Scope scope;

  static {
    IMPLICIT_ALLOWED_OPS = new HashMap<>();
    IMPLICIT_ALLOWED_OPS.put(new Operation("Describe"),
        Stream.of("Describe", "Read", "Write", "Delete", "Alter")
            .map(Operation::new).collect(Collectors.toSet()));
    IMPLICIT_ALLOWED_OPS.put(new Operation("DescribeConfigs"),
        Stream.of("DescribeConfigs", "AlterConfigs")
            .map(Operation::new).collect(Collectors.toSet()));
  }

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

  public void configureServerInfo(AuthorizerServerInfo serverInfo) {
    this.clusterId = serverInfo.clusterResource().clusterId();
    log.debug("Configuring scope for Kafka cluster with cluster id {}", clusterId);
    this.scope = Scope.kafkaClusterScope(clusterId);
    this.interBrokerListener = serverInfo.interBrokerEndpoint().listenerName().get();

    ConfluentAuthorizerConfig.Providers providers = authorizerConfig.createProviders(clusterId);
    providersCreated.addAll(providers.accessRuleProviders);
    if (providers.groupProvider != null)
      providersCreated.add(providers.groupProvider);
    if (providers.metadataProvider != null)
      providersCreated.add(providers.metadataProvider);

    configureProviders(providers.accessRuleProviders,
        providers.groupProvider,
        providers.metadataProvider,
        providers.auditLogProvider);

    initTimeout = authorizerConfig.initTimeout;
    if (groupProvider != null && groupProvider.usesMetadataFromThisKafkaCluster())
      usesMetadataFromThisKafkaCluster = true;
    else if (accessRuleProviders.stream()
        .anyMatch(AccessRuleProvider::usesMetadataFromThisKafkaCluster))
      usesMetadataFromThisKafkaCluster = true;
    else
      usesMetadataFromThisKafkaCluster =
          metadataProvider != null && metadataProvider.usesMetadataFromThisKafkaCluster();
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

  public CompletableFuture<Void> start(Map<String, ?> interBrokerListenerConfigs) {
    return start(interBrokerListenerConfigs, () -> { });
  }

  public CompletableFuture<Void> start(Map<String, ?> interBrokerListenerConfigs, Runnable initTask) {
    initTimeout = authorizerConfig.initTimeout;
    if (groupProvider != null && groupProvider.usesMetadataFromThisKafkaCluster())
      usesMetadataFromThisKafkaCluster = true;
    else if (accessRuleProviders.stream().anyMatch(AccessRuleProvider::usesMetadataFromThisKafkaCluster))
      usesMetadataFromThisKafkaCluster = true;
    else if (auditLogProvider.usesMetadataFromThisKafkaCluster())
      usesMetadataFromThisKafkaCluster = true;
    else
      usesMetadataFromThisKafkaCluster = metadataProvider != null && metadataProvider.usesMetadataFromThisKafkaCluster();

    Set<Provider> allProviders = new HashSet<>(); // Use a set to remove duplicates
    if (groupProvider != null)
      allProviders.add(groupProvider);
    allProviders.addAll(accessRuleProviders);
    if (metadataProvider != null)
      allProviders.add(metadataProvider);
    List<CompletableFuture<Void>> futures = allProviders.stream()
        .map(provider -> provider.start(interBrokerListenerConfigs))
        .map(CompletionStage::toCompletableFuture)
        .collect(Collectors.toList());
    CompletableFuture[] futureArray = new CompletableFuture[futures.size() + 1];
    futures.toArray(futureArray);
    futureArray[futures.size()] = initializeLicenseAsync(interBrokerListenerConfigs);
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
    this.auditLogProvider = auditLogProvider == null ? new DefaultAuditLogProvider() : auditLogProvider;
  }

  protected boolean ready() {
    return ready;
  }

  protected boolean isSuperUser(KafkaPrincipal principal, Action action) {
    return superUsers.contains(principal);
  }

  private AuthorizeResult authorize(RequestContext requestContext, Action action) {
    try {
      AuthorizePolicy authorizePolicy;
      KafkaPrincipal sessionPrincipal = requestContext.principal();
      String host = requestContext.clientAddress().getHostAddress();
      KafkaPrincipal userPrincipal = userPrincipal(sessionPrincipal);

      if (isSuperUser(userPrincipal, action)) {
        log.debug("principal = {} is a super user, allowing operation without checking any providers.", userPrincipal);
        authorizePolicy = new SuperUser(PolicyType.SUPER_USER, userPrincipal);
      } else {
        Set<KafkaPrincipal> groupPrincipals = groupProvider.groups(sessionPrincipal);
        Optional<KafkaPrincipal> superGroup = groupPrincipals.stream()
            .filter(group -> isSuperUser(group, action)).findFirst();
        if (superGroup.isPresent()) {
          log.debug("principal = {} belongs to super group {}, allowing operation without checking acls.",
              userPrincipal, superGroup.get());
          authorizePolicy = new SuperUser(PolicyType.SUPER_GROUP, superGroup.get());
        } else {
          authorizePolicy = authorize(sessionPrincipal, groupPrincipals, host, action);
        }
      }

      AuthorizeResult authorizeResult = authorizePolicy.policyType().accessGranted() ? AuthorizeResult.ALLOWED : AuthorizeResult.DENIED;
      logAuditMessage(requestContext, action, authorizeResult, authorizePolicy);
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
    Set<AccessRule> rules = new HashSet<>();
    accessRuleProviders.stream()
        .filter(AccessRuleProvider::mayDeny)
        .forEach(p -> rules.addAll(p.accessRules(sessionPrincipal, groupPrincipals, scope, action.resourcePattern())));

    // Check if there is any Deny acl match that would disallow this operation.
    Optional<AuthorizePolicy> authorizePolicy;
    if ((authorizePolicy = aclMatch(operation, resource, host, PermissionType.DENY, rules)).isPresent())
      return authorizePolicy.get();

    accessRuleProviders.stream()
        .filter(p -> !p.mayDeny())
        .forEach(p -> rules.addAll(p.accessRules(sessionPrincipal, groupPrincipals, scope, action.resourcePattern())));

    // Check if there are any Allow ACLs which would allow this operation.
    authorizePolicy = allowOps(operation).stream()
        .flatMap(op -> aclMatch(op, resource, host, PermissionType.ALLOW, rules).map(Stream::of).orElse(Stream.empty()))
        .findAny();

    return authorizePolicy.orElse(authorizePolicyWithNoMatchingRule(resource, rules));
  }

  @Override
  public void close() {
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

  private Optional<AuthorizePolicy> aclMatch(Operation op,
                                             ResourcePattern resource,
                                             String host, PermissionType permissionType,
                                             Collection<AccessRule> rules) {
    for (AccessRule rule : rules) {
      if (rule.permissionType().equals(permissionType)
          && (op.equals(rule.operation()) || rule.operation().equals(Operation.ALL))
          && (rule.host().equals(host) || rule.host().equals(AccessRule.ALL_HOSTS))) {
        AuthorizePolicy policy = rule.toAuthorizePolicy();
        log.debug("operation = {} on resource = {} from host = {} is {} based on policy = {}",
            op, resource, host, permissionType, policy);
        return Optional.of(policy);
      }
    }
    return Optional.empty();
  }

  private AuthorizePolicy authorizePolicyWithNoMatchingRule(ResourcePattern resource, Set<AccessRule> acls) {
    if (acls.isEmpty()) {
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

  private void logAuditMessage(RequestContext requestContext,
                               Action action,
                               AuthorizeResult authorizeResult,
                               AuthorizePolicy authorizePolicy) {
    auditLogProvider.logAuthorization(requestContext, action, authorizeResult, authorizePolicy);
  }

  // Allowing read, write, delete, or alter implies allowing describe.
  // See org.apache.kafka.common.acl.AclOperation for more details about ACL inheritance.
  private static Collection<Operation> allowOps(Operation operation) {
    Collection<Operation> allowOps = IMPLICIT_ALLOWED_OPS.get(operation);
    if (allowOps != null)
      return allowOps;
    else
      return Collections.singleton(operation);
  }

  private CompletableFuture<Void> initializeLicenseAsync(Map<String, ?> configs) {
    ExecutorService executor = Executors
        .newSingleThreadScheduledExecutor(ThreadUtils.createThreadFactory("license-%d", true));
    CompletableFuture<Void> licenseFuture = new CompletableFuture<>();
    executor.submit(() -> {
      try {
        initializeAndValidateLicense(configs);
        licenseFuture.complete(null);
      } catch (Throwable e) {
        log.error("License verification failed", e);
        licenseFuture.completeExceptionally(e);
      }
    });
    return licenseFuture.whenComplete((unused, e) -> executor.shutdownNow());
  }

  // Sub-classes may override to enforce license validation
  protected void initializeAndValidateLicense(Map<String, ?> configs) {
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
}
