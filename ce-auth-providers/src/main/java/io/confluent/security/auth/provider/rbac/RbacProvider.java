// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import io.confluent.security.auth.metadata.AuthCache;
import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.metadata.MetadataServer;
import io.confluent.security.auth.metadata.MetadataServiceConfig;
import io.confluent.security.auth.provider.ldap.LdapAuthenticateCallbackHandler;
import io.confluent.security.auth.provider.ldap.LdapConfig;
import io.confluent.security.auth.store.kafka.KafkaAuthStore;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.Authorizer;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.EmbeddedAuthorizer;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.security.authorizer.provider.GroupProvider;
import io.confluent.security.authorizer.provider.MetadataProvider;
import io.confluent.security.store.kafka.KafkaStoreConfig;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RbacProvider implements AccessRuleProvider, GroupProvider, MetadataProvider, ClusterResourceListener {
  private static final Logger log = LoggerFactory.getLogger(RbacProvider.class);

  private Map<String, ?> configs;
  private LdapAuthenticateCallbackHandler authenticateCallbackHandler;
  private Scope authScope;
  private Scope authStoreScope;
  private AuthStore authStore;
  private AuthCache authCache;

  private String clusterId;
  private MetadataServer metadataServer;
  private Collection<URL> metadataServerUrls;
  private Set<KafkaPrincipal> configuredSuperUsers;

  public RbacProvider() {
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
    if (clusterId == null)
      throw new IllegalStateException("Kafka cluster id not known");

    authStoreScope = Objects.requireNonNull(authScope, "authScope");
    if (providerName().equals(configs.get(ConfluentAuthorizerConfig.METADATA_PROVIDER_PROP))) {
      MetadataServiceConfig metadataServiceConfig = new MetadataServiceConfig(configs);
      metadataServer = createMetadataServer(metadataServiceConfig);
      metadataServerUrls = metadataServiceConfig.metadataServerUrls;

      Scope metadataScope = metadataServiceConfig.scope;

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
        ConfluentAuthorizerConfig.parseSuperUsers((String) configs.get(ConfluentAuthorizerConfig.SUPER_USERS_PROP));
  }

  @Override
  public String providerName() {
    return AccessRuleProviders.RBAC.name();
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
  public CompletionStage<Void> start(Map<String, ?> interBrokerListenerConfigs) {
    if (!usesMetadataFromThisKafkaCluster() && !configs.containsKey(KafkaStoreConfig.BOOTSTRAP_SERVERS_PROP)) {
      throw new ConfigException("Metadata bootstrap servers not specified for broker which does not host metadata service");
    }

    Map<String, Object> clientConfigs = new HashMap<>(configs);
    clientConfigs.putAll(interBrokerListenerConfigs);
    authStore = createAuthStore(authStoreScope, clientConfigs);
    this.authCache = authStore.authCache();
    if (LdapConfig.ldapEnabled(configs)) {
      authenticateCallbackHandler = new LdapAuthenticateCallbackHandler();
      authenticateCallbackHandler.configure(configs, "PLAIN", Collections.emptyList());
    }

    if (metadataServer != null)
      authStore.startService(metadataServerUrls);
    return authStore.startReader()
        .thenApply(unused -> {
          if (metadataServer != null)
            metadataServer.start(new RbacAuthorizer(), authStore, authenticateCallbackHandler);
          return null;
        });
  }

  @Override
  public boolean mayDeny() {
    return false;
  }

  @Override
  public boolean usesMetadataFromThisKafkaCluster() {
    return metadataServer != null;
  }

  @Override
  public boolean isSuperUser(KafkaPrincipal sessionPrincipal,
                             Set<KafkaPrincipal> groupPrincipals,
                             Scope scope) {
    return authCache.isSuperUser(scope, userPrincipal(sessionPrincipal), groupPrincipals);
  }

  @Override
  public Set<AccessRule> accessRules(KafkaPrincipal sessionPrincipal,
                                     Set<KafkaPrincipal> groupPrincipals,
                                     Scope scope,
                                     ResourcePattern resource) {
    return authCache.rbacRules(scope,
                               resource,
                               userPrincipal(sessionPrincipal),
                               groupPrincipals);
  }

  @Override
  public Set<KafkaPrincipal> groups(KafkaPrincipal sessionPrincipal) {
    return authCache.groups(userPrincipal(sessionPrincipal));
  }

  @Override
  public void close() {
    log.debug("Closing RBAC provider");
    AtomicReference<Throwable> firstException = new AtomicReference<>();
    Utils.closeQuietly(metadataServer, "metadataServer", firstException);
    Utils.closeQuietly(authStore, "authStore", firstException);
    if (authenticateCallbackHandler != null)
      Utils.closeQuietly(authenticateCallbackHandler, "authenticateCallbackHandler", firstException);
    Throwable exception = firstException.getAndSet(null);
    if (exception != null)
      throw new KafkaException("RbacProvider could not be closed cleanly", exception);
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
  public MetadataServer metadataServer() {
    return metadataServer;
  }

  // Allow override for testing
  protected AuthStore createAuthStore(Scope scope, Map<String, ?> configs) {
    KafkaAuthStore authStore = new KafkaAuthStore(scope);
    authStore.configure(configs);
    return authStore;
  }

  private MetadataServer createMetadataServer(MetadataServiceConfig metadataServiceConfig) {
    ServiceLoader<MetadataServer> servers = ServiceLoader.load(MetadataServer.class);
    MetadataServer metadataServer = null;
    for (MetadataServer server : servers) {
      if (server.providerName().equals(providerName())) {
        metadataServer = server;
        break;
      }
    }
    if (metadataServer == null)
      metadataServer = new DummyMetadataServer();
    if (metadataServer instanceof ClusterResourceListener) {
      ((ClusterResourceListener) metadataServer).onUpdate(new ClusterResource(clusterId));
    }
    metadataServer.configure(metadataServiceConfig.metadataServerConfigs());
    return metadataServer;
  }

  private class RbacAuthorizer extends EmbeddedAuthorizer {
    RbacAuthorizer() {
      configureProviders(Collections.singletonList(RbacProvider.this), RbacProvider.this, null);
      configureSuperUsers(configuredSuperUsers);
    }
  }

  private static class DummyMetadataServer implements MetadataServer {

    @Override
    public void start(Authorizer embeddedAuthorizer, AuthStore authStore, AuthenticateCallbackHandler callbackHandler) {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void close() throws IOException {
    }
  }
}
