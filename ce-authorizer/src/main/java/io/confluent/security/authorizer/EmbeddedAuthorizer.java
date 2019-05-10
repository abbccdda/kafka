// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import io.confluent.common.license.LicenseExpiredException;
import io.confluent.common.license.InvalidLicenseException;
import io.confluent.common.license.LicenseValidator;
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
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cross-component embedded authorizer that implements common authorization logic. This
 * authorizer loads configured providers and uses them to perform authorization.
 */
public class EmbeddedAuthorizer implements Authorizer, ClusterResourceListener {

  protected static final Logger log = LoggerFactory.getLogger("kafka.authorizer.logger");

  private static final LicenseValidator DUMMY_LICENSE_VALIDATOR = new DummyLicenseValidator();

  private static final String ZK_CONNECT_PROPNAME = "zookeeper.connect";
  private static final String METRIC_GROUP = "confluent.license";
  private static final Map<Operation, Collection<Operation>> IMPLICIT_ALLOWED_OPS;

  private final Time time;
  private final Set<Provider> providersCreated;
  private LicenseValidator licenseValidator;
  private GroupProvider groupProvider;
  private List<AccessRuleProvider> accessRuleProviders;
  private MetadataProvider metadataProvider;
  private boolean allowEveryoneIfNoAcl;
  private Set<KafkaPrincipal> superUsers;
  private Duration initTimeout;
  private boolean usesMetadataFromThisKafkaCluster;
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
    this(Time.SYSTEM);
  }

  public EmbeddedAuthorizer(Time time) {
    this.time = time;
    this.providersCreated = new HashSet<>();
    this.superUsers = Collections.emptySet();
    this.scope = Scope.ROOT_SCOPE; // Scope is only required for broker authorizers using RBAC
    licenseValidator = licenseValidator();
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

    if (licenseValidator == null)
      licenseValidator = DUMMY_LICENSE_VALIDATOR;
    initializeAndValidateLicense(configs, licensePropName());

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

  public CompletableFuture<Void> start() {
    Set<Provider> providers = new HashSet<>(); // Use a set to remove duplicates
    if (groupProvider != null)
      providers.add(groupProvider);
    providers.addAll(accessRuleProviders);
    if (metadataProvider != null)
      providers.add(metadataProvider);
    List<CompletableFuture<Void>> futures = providers.stream()
        .map(Provider::start).map(CompletionStage::toCompletableFuture)
        .collect(Collectors.toList());
    CompletableFuture<Void> readyFuture =
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]));
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

  protected void configureSuperUsers(Set<KafkaPrincipal> superUsers) {
    this.superUsers = new HashSet<>(superUsers);
  }

  private AuthorizeResult authorize(KafkaPrincipal sessionPrincipal, String host, Action action) {
    try {
      // On license expiry, update metric and log error, but continue to authorize
      licenseValidator.verifyLicense(false);

      boolean authorized;
      KafkaPrincipal userPrincipal = userPrincipal(sessionPrincipal);
      if (superUsers.contains(userPrincipal)) {
        log.debug("principal = {} is a super user, allowing operation without checking any providers.", userPrincipal);
        authorized = true;
      } else {
        Set<KafkaPrincipal> groupPrincipals = groupProvider.groups(sessionPrincipal);
        Optional<KafkaPrincipal> superGroup = groupPrincipals.stream().filter(superUsers::contains).findFirst();
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
        ClientUtils.closeQuietly(provider, provider.providerName(), firstException));
    ClientUtils.closeQuietly(licenseValidator, "licenseValidator", firstException);
    Throwable exception = firstException.getAndSet(null);
    // We don't want to prevent clean broker shutdown if providers are not gracefully closed.
    if (exception != null)
      log.error("Failed to close authorizer cleanly", exception);
  }

  protected Scope scope() {
    return scope;
  }

  // Allow authorizer implementation to override so that LdapAuthorizer can provide its custom property
  protected String licensePropName() {
    return ConfluentAuthorizerConfig.LICENSE_PROP;
  }

  // Allow authorizer implementation to override so that LdapAuthorizer can provide its custom metric
  protected String licenseStatusMetricGroup() {
    return METRIC_GROUP;
  }

  // Allow Kafka brokers to override license validator. Other services (e.g. Metadata service)
  // will be performing license validation separately, so use a dummy validator as default.
  protected LicenseValidator licenseValidator() {
    return DUMMY_LICENSE_VALIDATOR;
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

  private void initializeAndValidateLicense(Map<String, ?> configs, String licensePropName) {
    String license = (String) configs.get(licensePropName);
    String zkConnect = (String) configs.get(ZK_CONNECT_PROPNAME);
    try {
      licenseValidator.initializeAndVerify(license, zkConnect, time, licenseStatusMetricGroup());
    } catch (InvalidLicenseException | LicenseExpiredException e) {
      throw new InvalidLicenseException(
          String.format("Confluent Authorizer license validation failed."
              + " Please specify a valid license in the config " + licensePropName
              + " to enable authorization using %s. Kafka brokers may be started with basic"
              + " user-principal based authorization using 'kafka.security.auth.SimpleAclAuthorizer'"
              + " without a license.", this.getClass().getName()), e);
    }
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

  private static class DummyLicenseValidator implements LicenseValidator {

    @Override
    public void initializeAndVerify(String license, String zkConnect, Time time, String metricGroup) {
    }

    @Override
    public void verifyLicense(boolean failOnError) {
    }

    @Override
    public void close() {
    }
  }
}
