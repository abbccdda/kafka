// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider;

import io.confluent.security.auth.client.acl.MdsAclMigration;
import io.confluent.security.auth.metadata.AuthCache;
import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.metadata.AuthWriter;
import io.confluent.security.auth.provider.ldap.LdapAuthenticateCallbackHandler;
import io.confluent.security.auth.provider.ldap.LdapConfig;
import io.confluent.security.auth.store.kafka.KafkaAuthStore;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.AclMigrationAware;
import io.confluent.security.authorizer.Action;
import io.confluent.security.authorizer.Authorizer;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.EmbeddedAuthorizer;
import io.confluent.security.authorizer.Operation;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.ResourceType;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.AuditLogProvider;
import io.confluent.security.authorizer.provider.Auditable;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.security.authorizer.provider.GroupProvider;
import io.confluent.security.authorizer.provider.MetadataProvider;
import io.confluent.security.store.NotMasterWriterException;
import io.confluent.security.store.kafka.KafkaStoreConfig;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfluentAdmin;
import org.apache.kafka.clients.admin.CreateAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsOptions;
import org.apache.kafka.clients.admin.DeleteAclsResult.FilterResults;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;
import org.apache.kafka.server.http.MetadataServer;
import org.apache.kafka.server.http.MetadataServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfluentProvider implements AccessRuleProvider, GroupProvider, MetadataProvider,
    org.apache.kafka.server.authorizer.Authorizer, ClusterResourceListener,
    Auditable, AclMigrationAware {
  private static final Logger log = LoggerFactory.getLogger(ConfluentProvider.class);

  static final ResourceType SECURITY_METADATA = new ResourceType("SecurityMetadata");
  private static final Set<ResourceType> METADATA_RESOURCE_TYPES = Utils.mkSet(SECURITY_METADATA);
  private static final Set<Operation> METADATA_OPS = Utils.mkSet(
      new Operation("DescribeAccess"),
      new Operation("AlterAccess")
  );

  private Map<String, ?> configs;
  private MetadataServerConfig metadataServerConfig;
  private LdapAuthenticateCallbackHandler authenticateCallbackHandler;
  private AuditLogProvider auditLogProvider;
  private Scope authScope;
  private Scope authStoreScope;
  private AuthStore authStore;
  private AuthCache authCache;
  private Optional<ConfluentAdmin> aclClient = Optional.empty();

  private String clusterId;
  private Set<KafkaPrincipal> configuredSuperUsers;

  private MetadataServer metadataServer;

  public ConfluentProvider() {
    this.authScope = Scope.ROOT_SCOPE;
  }

  @Override
  public void onUpdate(ClusterResource clusterResource) {
    this.clusterId = clusterResource.clusterId();
    this.authScope = Scope.kafkaClusterScope(clusterId);
    this.authScope.validate(false);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.configs = configs;
    metadataServerConfig = new MetadataServerConfig(configs);
    if (clusterId == null)
      throw new IllegalStateException("Kafka cluster id not known");

    authStoreScope = Objects.requireNonNull(authScope, "authScope");
    if (usesMetadataFromThisKafkaCluster()) {
      Scope metadataScope = Scope.ROOT_SCOPE;
      // If authorizer scope is defined, then it must be contained within the metadata server
      // scope. We use the metadata server scope for the single AuthStore shared by the authorizer
      // on this broker and the metadata server. If the broker authorizer is not RBAC-enabled,
      // then authScope may be empty and we can just use the metadata server scope for the store.
      if (!metadataScope.containsScope(authScope) && !Scope.ROOT_SCOPE.equals(authScope))
        throw new ConfigException(String.format("Metadata service scope %s does not contain broker scope %s",
            metadataScope, authScope));
      authStoreScope = metadataScope;
    }
    // Allow security metadata access for broker's configured super-user in the metadata cluster
    this.configuredSuperUsers =
        ConfluentAuthorizerConfig.parseUsers((String) configs.get(ConfluentAuthorizerConfig.SUPER_USERS_PROP));
  }

  @Override
  public String providerName() {
    return AccessRuleProviders.CONFLUENT.name();
  }

  /**
   * Brokers running RBAC should be either:
   *   - in the metadata cluster, running MDS. These should have metadata server listeners configured.
   *   - in another cluster. These should have metadata bootstrap servers configured.
   */
  @Override
  public boolean providerConfigured(Map<String, ?> configs) {
    return new MetadataServerConfig(configs).isMetadataServerEnabled() ||
        configs.containsKey(KafkaStoreConfig.BOOTSTRAP_SERVERS_PROP);
  }

  @Override
  public void registerMetadataServer(MetadataServer metadataServer) {
    if (usesMetadataFromThisKafkaCluster()) {
      this.metadataServer = metadataServer;
    }
  }

  /**
   * Starts the RBAC provider.
   * <p>
   * On brokers running metadata service, the start up sequence is:
   * <ol>
   *   <li>Start the metadata writer coordinator.</li>
   *   <li>Master writer is started when writer is elected. First master writer creates the auth topic.</li>
   *   <li>Start reader. Reader waits for topic to be created and then consumes from topic partitions.</li>
   *   <li>Writer reads any external store, writes entries to auth topic and then updates status for
   *       all its partitions by writing initialized status entry to the partitions.</li>
   *   <li>Reader completes start up when it sees the initialized status of writer on all partitions.</li>
   *   <li>Start metadata server to support authorization in other components.</li>
   *   <li>Complete the returned CompletionStage. Inter-broker listener is required from 1),
   *       but other listeners are started only at this point.</li>
   * </ol>
   *
   * On brokers in other clusters, the reader starts up and waits for the writer on the
   * metadata cluster to create and initialize the topic.
   */
  @Override
  public CompletionStage<Void> start(AuthorizerServerInfo serverInfo,
                                     Map<String, ?> interBrokerListenerConfigs) {
    if (!providerConfigured(interBrokerListenerConfigs)) {
      throw new ConfigException("Metadata bootstrap servers not specified for broker which does not host metadata service");
    }

    Map<String, Object> clientConfigs = new HashMap<>(configs);
    clientConfigs.putAll(interBrokerListenerConfigs);
    authStore = createAuthStore(authStoreScope, serverInfo, clientConfigs);
    this.authCache = authStore.authCache();
    if (LdapConfig.ldapEnabled(configs)) {
      authenticateCallbackHandler = new LdapAuthenticateCallbackHandler();
      authenticateCallbackHandler.configure(configs, "PLAIN", Collections.emptyList());
    }

    if (usesMetadataFromThisKafkaCluster()) {
      List<URL> advertisedUrls = metadataServerConfig.metadataServerAdvertisedListeners();
      authStore.startService(
          !advertisedUrls.isEmpty()
              ? advertisedUrls
              : metadataServerConfig.metadataServerListeners());
    }

    return authStore.startReader()
        .thenApply(unused -> {
          if (metadataServer != null) {
            SimpleInjector injector = new SimpleInjector();
            injector.putInstance(Authorizer.class, createRbacAuthorizer());
            injector.putInstance(AuthStore.class, authStore);
            injector.putInstance(AuthenticateCallbackHandler.class, authenticateCallbackHandler);
            metadataServer.registerMetadataProvider(providerName(), injector);
          }

          Set<String> accessProviders = Utils.mkSet(((String)
              configs.get(ConfluentAuthorizerConfig.ACCESS_RULE_PROVIDERS_PROP)).trim().split("\\s*,\\s*"));
          if (accessProviders.contains(AccessRuleProviders.ZK_ACL.name()) ||
              accessProviders.contains(AccessRuleProviders.CONFLUENT.name())) {
            aclClient = Optional.of(createMdsAdminClient(serverInfo, clientConfigs));
          }
          return null;
        });
  }

  @Override
  public boolean mayDeny() {
    return true;
  }

  /**
   * Returns true if this broker is running the RBAC service in the embedded {@link MetadataServer},
   * or false if the {@link AuthStore} is listening to RBAC service in another cluster instead.
   */
  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    return metadataServerConfig.isMetadataServerEnabled()
        && !configs.containsKey(KafkaStoreConfig.BOOTSTRAP_SERVERS_PROP);
  }

  @Override
  public boolean isSuperUser(KafkaPrincipal principal,
                             Scope scope) {
    return false; // All roles are handled using access rules from the policy
  }

  @Override
  public Set<AccessRule> accessRules(KafkaPrincipal sessionPrincipal,
                                     Set<KafkaPrincipal> groupPrincipals,
                                     Scope scope,
                                     ResourcePattern resource) {
    Set<AccessRule> rbacRules = authCache.rbacRules(scope,
        resource,
        userPrincipal(sessionPrincipal),
        groupPrincipals);

    Set<AccessRule> aclRules = authCache.aclRules(scope,
        resource,
        userPrincipal(sessionPrincipal),
        groupPrincipals);

    rbacRules.addAll(aclRules);
    return rbacRules;
  }

  @Override
  public Set<KafkaPrincipal> groups(KafkaPrincipal sessionPrincipal) {
    return authCache.groups(userPrincipal(sessionPrincipal));
  }

  @Override
  public void close() {
    log.debug("Closing RBAC provider");
    AtomicReference<Throwable> firstException = new AtomicReference<>();
    Utils.closeQuietly(authStore, "authStore", firstException);
    if (authenticateCallbackHandler != null)
      Utils.closeQuietly(authenticateCallbackHandler, "authenticateCallbackHandler", firstException);
    Utils.closeQuietly(aclClient.orElse(null), "aclClient", firstException);
    Throwable exception = firstException.getAndSet(null);
    if (exception != null)
      throw new KafkaException("ConfluentProvider could not be closed cleanly", exception);
  }

  @Override
  public void auditLogProvider(AuditLogProvider auditLogProvider) {
    this.auditLogProvider = auditLogProvider;
  }

  private KafkaPrincipal userPrincipal(KafkaPrincipal sessionPrincipal) {
    return sessionPrincipal.getClass() != KafkaPrincipal.class
        ? new KafkaPrincipal(sessionPrincipal.getPrincipalType(), sessionPrincipal.getName())
        : sessionPrincipal;
  }

  // Visibility for testing
  public AuthStore authStore() {
    return authStore;
  }

  // Visibility for testing
  EmbeddedAuthorizer createRbacAuthorizer() {
    return new RbacAuthorizer();
  }

  protected ConfluentAdmin createMdsAdminClient(AuthorizerServerInfo serverInfo, Map<String, ?> clientConfigs) {
    Map<String, Object> adminClientConfigs = new KafkaStoreConfig(serverInfo, clientConfigs).adminClientConfigs();
    return (ConfluentAdmin) Admin.create(adminClientConfigs);
  }

  // Allow override for testing
  protected AuthStore createAuthStore(Scope scope, AuthorizerServerInfo serverInfo, Map<String, ?> configs) {
    KafkaAuthStore authStore = new KafkaAuthStore(scope, serverInfo);
    authStore.configure(configs);
    return authStore;
  }

  @Override
  public Map<Endpoint, ? extends CompletionStage<Void>> start(final AuthorizerServerInfo serverInfo) {
    return Collections.emptyMap();
  }

  @Override
  public List<AuthorizationResult> authorize(final AuthorizableRequestContext requestContext,
                                             final List<org.apache.kafka.server.authorizer.Action> actions) {
    throw new IllegalStateException("This provider should be used for authorization only using the AccessRuleProvider interface");
  }

  @Override
  public List<? extends CompletionStage<AclCreateResult>> createAcls(final AuthorizableRequestContext requestContext,
                                                                     final List<AclBinding> aclBindings) {
    return createAcls(requestContext, aclBindings, Optional.empty());
  }

  @Override
  public List<? extends CompletionStage<AclCreateResult>> createAcls(final AuthorizableRequestContext requestContext,
                                                                     final List<AclBinding> aclBindings,
                                                                     final Optional<String> aclClusterId) {
    if (!aclClient.isPresent())
      throw new IllegalStateException("Acl operations are not supported by this provider");
    ConfluentAdmin adminClient = aclClient.get();

    AuthWriter writer = authStore.writer();
    List<CompletableFuture<AclCreateResult>> results = new ArrayList<>(aclBindings.size());
    if (aclClusterId.isPresent()) {
      // This is an internal request from a broker to update centralized ACL for the specified cluster
      // Perform the update if this broker is the master writer.
      if (writer == null || !authStore.isMasterWriter()) {
        CompletableFuture<AclCreateResult> result = CompletableFuture
            .completedFuture(new AclCreateResult(new NotMasterWriterException("Current master writer is " + authStore.masterWriterId())));
        for (int i = 0; i < aclBindings.size(); i++) {
          results.add(result);
        }
      } else {
        for (int i = 0; i < aclBindings.size(); i++) {
          results.add(null);
        }
        writer.createAcls(Scope.kafkaClusterScope(aclClusterId.get()), aclBindings)
            .forEach((binding, result) ->
                results.set(aclBindings.indexOf(binding), result.toCompletableFuture()));
      }
    } else {
      // This is an external AdminClient request to update ACLs for this broker. Send the request
      // to the MDS master writer broker as a Kafka CreateAclsRequest that includes the cluster id
      // of this broker.
      Integer writerId = authStore.masterWriterId();
      if (writerId == null) {
        CompletableFuture<AclCreateResult> result = CompletableFuture
            .completedFuture(new AclCreateResult(new RebalanceInProgressException("Writer election is in progress")));
        for (int i = 0; i < aclBindings.size(); i++) {
          results.add(result);
        }
      } else {
        Map<AclBinding, KafkaFuture<Void>> futures = adminClient.createCentralizedAcls(aclBindings,
            new CreateAclsOptions(),
            clusterId,
            writerId).values();
        for (AclBinding aclBinding : aclBindings) {
          CompletableFuture<AclCreateResult> result = new CompletableFuture<>();
          results.add(result);
          futures.get(aclBinding).whenComplete((unused, throwable) -> {
            if (throwable == null)
              result.complete(new AclCreateResult(null));
            else
              result.complete(new AclCreateResult(toApiException(throwable)));
          });
        }
      }
    }
    return results;
  }

  @Override
  public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(final AuthorizableRequestContext requestContext,
                                                                     final List<AclBindingFilter> aclBindingFilters) {
    return deleteAcls(requestContext, aclBindingFilters, Optional.empty());
  }

  @Override
  public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(final AuthorizableRequestContext requestContext,
                                                                     final List<AclBindingFilter> aclBindingFilters,
                                                                     final Optional<String> aclClusterId) {
    if (!aclClient.isPresent())
      throw new IllegalStateException("Acl operations are not supported by this provider");
    ConfluentAdmin adminClient = aclClient.get();

    AuthWriter writer = authStore.writer();
    List<CompletableFuture<AclDeleteResult>> results = new ArrayList<>(aclBindingFilters.size());
    if (aclClusterId.isPresent()) {
      // This is an internal request from a broker to update centralized ACL for the specified cluster
      // Perform the update if this broker is the master writer. No further resource access is checked
      // before delete since Cluster->Alter has already been authorized.
      if (writer == null || !authStore.isMasterWriter()) {
        CompletableFuture<AclDeleteResult> result = CompletableFuture
            .completedFuture(new AclDeleteResult(new NotMasterWriterException("Current master writer is " + authStore.masterWriterId())));
        for (int i = 0; i < aclBindingFilters.size(); i++) {
          results.add(result);
        }
      } else {
        for (int i = 0; i < aclBindingFilters.size(); i++) {
          results.add(null);
        }
        writer.deleteAcls(Scope.kafkaClusterScope(aclClusterId.get()), aclBindingFilters, r -> true)
            .forEach((filter, result) ->
                results.set(aclBindingFilters.indexOf(filter), result.toCompletableFuture()));
      }
    } else {
      // This is an external AdminClient request to update ACLs for this broker. Send the request
      // to the MDS master writer broker as a Kafka DeleteAclsRequest that includes the cluster id
      // of this broker.
      Integer writerId = authStore.masterWriterId();
      if (writerId == null) {
        CompletableFuture<AclDeleteResult> result = CompletableFuture
            .completedFuture(new AclDeleteResult(new RebalanceInProgressException("Writer election is in progress")));
        for (int i = 0; i < aclBindingFilters.size(); i++) {
          results.add(result);
        }
      } else {
        Map<AclBindingFilter, KafkaFuture<FilterResults>> futures = adminClient.deleteCentralizedAcls(aclBindingFilters,
            new DeleteAclsOptions(),
            clusterId,
            writerId).values();
        for (AclBindingFilter aclBindingFilter : aclBindingFilters) {
          CompletableFuture<AclDeleteResult> result = new CompletableFuture<>();
          results.add(result);
          futures.get(aclBindingFilter).whenComplete((deleteResult, throwable) -> {
            if (throwable == null)
              result.complete(new AclDeleteResult(deleteResult.values().stream().map(acl ->
                  new AclDeleteResult.AclBindingDeleteResult(acl.binding())).collect(Collectors.toList())));
            else
              result.complete(new AclDeleteResult(toApiException(throwable)));
          });
        }
      }
    }
    return results;
  }

  @Override
  public Iterable<AclBinding> acls(final AclBindingFilter filter) {
    if (!aclClient.isPresent())
      throw new IllegalStateException("Acl operations are not supported by this provider");

    return authCache.aclBindings(authScope, filter, r -> true);
  }

  @Override
  public Runnable migrationTask(final org.apache.kafka.server.authorizer.Authorizer sourceAuthorizer) {
    return () -> {
      MdsAclMigration aclMigration = new MdsAclMigration(clusterId, () -> authStore.masterWriterId());
      if (!aclClient.isPresent())
        throw new IllegalStateException("ACL provider is not enabled");
      try {
        aclMigration.migrate(configs, sourceAuthorizer, aclClient.get());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  private ApiException toApiException(Throwable throwable) {
    return throwable instanceof ApiException ? (ApiException) throwable : new ApiException(throwable);
  }

  private class RbacAuthorizer extends EmbeddedAuthorizer {
    RbacAuthorizer() {
      configureProviders(Collections.singletonList(ConfluentProvider.this), ConfluentProvider.this, null, auditLogProvider);
    }

    /**
     * Users configured as `super.users` on the brokers running metadata service are
     * granted access to security metadata for all clusters. This helps with bootstrapping
     * new clusters, allowing role bindings to be created for new clusters using
     * these super user principals.
     *
     * Note that `super.users` also have access to all broker resources in the metadata
     * cluster, but these are handled by the broker authorizer.
     */
    @Override
    protected boolean isSuperUser(KafkaPrincipal principal, Action action) {
      return configuredSuperUsers.contains(principal) &&
          (METADATA_RESOURCE_TYPES.contains(action.resourceType()) || METADATA_OPS.contains(action.operation()));
    }
  }

  private static final class SimpleInjector implements MetadataServer.Injector {

    private final HashMap<Class<?>, Object> instances = new HashMap<>();

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getInstance(Class<T> clazz) {
      return (T) instances.get(clazz);
    }

    private <T> void putInstance(Class<T> clazz, T instance) {
      instances.put(clazz, instance);
    }
  }
}
