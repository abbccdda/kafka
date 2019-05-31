// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import io.confluent.security.authorizer.provider.AccessRuleProvider;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cross-component embedded authorizer that implements common authorization logic. This
 * authorizer loads configured providers and uses them to perform authorization.
 */
public class EmbeddedAuthorizer implements Authorizer, ClusterResourceListener {

  protected static final Logger log = LoggerFactory.getLogger("kafka.authorizer.logger");

  private static final Map<Operation, Collection<Operation>> IMPLICIT_ALLOWED_OPS;

  private final Set<Provider> providersCreated;
  private GroupProvider groupProvider;
  private List<AccessRuleProvider> accessRuleProviders;
  private MetadataProvider metadataProvider;
  private boolean allowEveryoneIfNoAcl;
  private Set<KafkaPrincipal> superUsers;
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
    this.scope = Scope.ROOT_SCOPE; // Scope is only required for broker authorizers using RBAC
  }

  @Override
  public void onUpdate(ClusterResource clusterResource) {
    String clusterId = clusterResource.clusterId();
    log.debug("Configuring scope for Kafka cluster with cluster id {}", clusterId);
    this.clusterId = clusterId;
    this.scope = Scope.kafkaClusterScope(clusterId);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    ConfluentAuthorizerConfig authorizerConfig = new ConfluentAuthorizerConfig(configs);
    allowEveryoneIfNoAcl = authorizerConfig.allowEveryoneIfNoAcl;
    superUsers = authorizerConfig.superUsers;

    ConfluentAuthorizerConfig.Providers providers = authorizerConfig.createProviders(clusterId);
    providersCreated.addAll(providers.accessRuleProviders);
    if (providers.groupProvider != null)
      providersCreated.add(providers.groupProvider);
    if (providers.metadataProvider != null)
      providersCreated.add(providers.metadataProvider);

    configureProviders(providers.accessRuleProviders,
        providers.groupProvider,
        providers.metadataProvider);

    initTimeout = authorizerConfig.initTimeout;
    if (groupProvider != null && groupProvider.usesMetadataFromThisKafkaCluster())
      usesMetadataFromThisKafkaCluster = true;
    else if (accessRuleProviders.stream().anyMatch(AccessRuleProvider::usesMetadataFromThisKafkaCluster))
      usesMetadataFromThisKafkaCluster = true;
    else
      usesMetadataFromThisKafkaCluster = metadataProvider != null && metadataProvider.usesMetadataFromThisKafkaCluster();
  }

  @Override
  public List<AuthorizeResult> authorize(KafkaPrincipal sessionPrincipal, String host, List<Action> actions) {
    return  actions.stream()
        .map(action -> authorize(sessionPrincipal, host, action))
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

  public CompletableFuture<Void> start(Map<String, ?> interBrokerListenerConfigs) {
    Set<Provider> providers = new HashSet<>(); // Use a set to remove duplicates
    if (groupProvider != null)
      providers.add(groupProvider);
    providers.addAll(accessRuleProviders);
    if (metadataProvider != null)
      providers.add(metadataProvider);
    List<CompletableFuture<Void>> futures = providers.stream()
        .map(provider -> provider.start(interBrokerListenerConfigs))
        .map(CompletionStage::toCompletableFuture)
        .collect(Collectors.toList());
    CompletableFuture<Void> readyFuture =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
            .thenApply(unused -> {
              initializeAndValidateLicense(interBrokerListenerConfigs);
              this.ready = true;
              return null;
            });
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

  protected List<AccessRuleProvider> accessRuleProviders() {
    return accessRuleProviders;
  }

  protected void configureProviders(List<AccessRuleProvider> accessRuleProviders,
                                    GroupProvider groupProvider,
                                    MetadataProvider metadataProvider) {
    this.accessRuleProviders = accessRuleProviders;
    this.groupProvider = groupProvider;
    this.metadataProvider = metadataProvider;
  }

  protected boolean ready() {
    return ready;
  }

  protected boolean isSuperUser(KafkaPrincipal principal, Action action) {
    return superUsers.contains(principal);
  }

  private AuthorizeResult authorize(KafkaPrincipal sessionPrincipal, String host, Action action) {
    try {
      boolean authorized;
      KafkaPrincipal userPrincipal = userPrincipal(sessionPrincipal);
      if (isSuperUser(userPrincipal, action)) {
        log.debug("principal = {} is a super user, allowing operation without checking any providers.", userPrincipal);
        authorized = true;
      } else {
        Set<KafkaPrincipal> groupPrincipals = groupProvider.groups(sessionPrincipal);
        Optional<KafkaPrincipal> superGroup = groupPrincipals.stream()
            .filter(group -> isSuperUser(group, action)).findFirst();
        if (superGroup.isPresent()) {
          log.debug("principal = {} belongs to super group {}, allowing operation without checking acls.",
              userPrincipal, superGroup.get());
          authorized = true;
        } else {
          authorized = authorize(sessionPrincipal, groupPrincipals, host, action);
        }
      }
      logAuditMessage(sessionPrincipal, authorized, action.operation(), action.resourcePattern(), host);
      return authorized ? AuthorizeResult.ALLOWED : AuthorizeResult.DENIED;

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

  private boolean authorize(KafkaPrincipal sessionPrincipal,
      Set<KafkaPrincipal> groupPrincipals,
      String host,
      Action action) {

    if (accessRuleProviders.stream()
        .anyMatch(p -> p.isSuperUser(sessionPrincipal, groupPrincipals, action.scope()))) {
      return true;
    }

    ResourcePattern resource = action.resourcePattern();
    Operation operation = action.operation();
    Set<AccessRule> rules = new HashSet<>();
    accessRuleProviders.stream()
        .filter(AccessRuleProvider::mayDeny)
        .forEach(p -> rules.addAll(p.accessRules(sessionPrincipal, groupPrincipals, action.scope(), action.resourcePattern())));

    // Check if there is any Deny acl match that would disallow this operation.
    if (aclMatch(operation, resource, host, PermissionType.DENY, rules))
      return false;

    accessRuleProviders.stream()
        .filter(p -> !p.mayDeny())
        .forEach(p -> rules.addAll(p.accessRules(sessionPrincipal, groupPrincipals, action.scope(), action.resourcePattern())));

    // Check if there are any Allow ACLs which would allow this operation.
    if (allowOps(operation).stream().anyMatch(op -> aclMatch(op, resource, host, PermissionType.ALLOW, rules)))
      return true;

    return isEmptyAclAndAuthorized(resource, rules);
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

  private boolean aclMatch(Operation op,
      ResourcePattern resource,
      String host, PermissionType permissionType,
      Collection<AccessRule> permissions) {
    for (AccessRule acl : permissions) {
      if (acl.permissionType().equals(permissionType)
          && (op.equals(acl.operation()) || acl.operation().equals(Operation.ALL))
          && (acl.host().equals(host) || acl.host().equals(AccessRule.ALL_HOSTS))) {
        log.debug("operation = {} on resource = {} from host = {} is {} based on acl = {}",
            op, resource, host, permissionType, acl.sourceDescription());
        return true;
      }
    }
    return false;
  }

  private boolean isEmptyAclAndAuthorized(ResourcePattern resource, Set<AccessRule> acls) {
    if (acls.isEmpty()) {
      log.debug("No acl found for resource {}, authorized = {}", resource, allowEveryoneIfNoAcl);
      return allowEveryoneIfNoAcl;
    } else {
      return false;
    }
  }

  private KafkaPrincipal userPrincipal(KafkaPrincipal sessionPrincipal) {
    return sessionPrincipal.getClass() != KafkaPrincipal.class
        ? new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName())
        : sessionPrincipal;
  }

  /**
   * Log using the same format as SimpleAclAuthorizer:
   * <pre>
   *  def logMessage: String = {
   *    val authResult = if (authorized) "Allowed" else "Denied"
   *    s"Principal = $principal is $authResult Operation = $operation from host = $host on
   * resource
   * = $resource"
   *  }
   * </pre>
   */
  private void logAuditMessage(KafkaPrincipal principal, boolean authorized,
      Operation op,
      ResourcePattern resource, String host) {
    String logMessage = "Principal = {} is {} Operation = {} from host = {} on resource = {}";
    if (authorized) {
      log.debug(logMessage, principal, "Allowed", op, host, resource);
    } else {
      log.info(logMessage, principal, "Denied", op, host, resource);
    }
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
