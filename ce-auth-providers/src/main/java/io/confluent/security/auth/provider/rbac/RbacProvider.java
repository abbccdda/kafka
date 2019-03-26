// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.provider.rbac;

import io.confluent.security.authorizer.Authorizer;
import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import io.confluent.security.authorizer.EmbeddedAuthorizer;
import io.confluent.security.authorizer.Resource;
import io.confluent.security.authorizer.provider.ConfluentBuiltInProviders.AccessRuleProviders;
import io.confluent.security.authorizer.provider.AccessRuleProvider;
import io.confluent.security.authorizer.provider.GroupProvider;
import io.confluent.security.authorizer.AccessRule;
import io.confluent.security.authorizer.provider.MetadataProvider;
import io.confluent.security.auth.metadata.AuthCache;
import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.metadata.MetadataServer;
import io.confluent.security.auth.metadata.MetadataServiceConfig;
import io.confluent.security.auth.store.kafka.KafkaAuthStore;
import io.confluent.security.rbac.Scope;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.ClientUtils;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RbacProvider implements AccessRuleProvider, GroupProvider, MetadataProvider {
  private static final Logger log = LoggerFactory.getLogger(RbacProvider.class);

  private AuthStore authStore;
  private AuthCache authCache;

  private MetadataServer metadataServer;
  private Collection<URL> metadataServerUrls;

  @Override
  public void configure(Map<String, ?> configs) {
    String scope = (String) configs.get(ConfluentAuthorizerConfig.SCOPE_PROP);
    Scope authScope;
    try {
      authScope = new Scope(scope);
    } catch (Exception e) {
      throw new ConfigException("Invalid scope for RBAC provider: " + scope, e);
    }

    Scope authStoreScope = authScope;
    if (providerName().equals(configs.get(ConfluentAuthorizerConfig.METADATA_PROVIDER_PROP))) {
      MetadataServiceConfig metadataServiceConfig = new MetadataServiceConfig(configs);
      metadataServer = createMetadataServer(metadataServiceConfig);
      metadataServerUrls = metadataServiceConfig.metadataServerUrls;

      Scope metadataScope = metadataServiceConfig.scope;

      // If authorizer scope is defined, then it must be contained within the metadata server
      // scope. We use the metadata server scope for the single AuthStore shared by the authorizer
      // on this broker and the metadata server. If the broker authorizer is not RBAC-enabled,
      // then authScope may be empty and we can just use the metadata server scope for the store.
      if (!metadataScope.containsScope(authScope) && !authScope.name().isEmpty())
        throw new ConfigException(String.format("Metadata service scope %s does not contain broker scope %s",
            metadataScope, authScope));
      authStoreScope = metadataScope;
    }
    authStore = createAuthStore(authStoreScope, configs);
    this.authCache = authStore.authCache();
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
  public CompletionStage<Void> start() {
    if (metadataServer != null)
      authStore.startService(metadataServerUrls);
    return authStore.startReader()
        .thenApply(unused -> {
          if (metadataServer != null)
            metadataServer.start(new RbacAuthorizer(), authStore);
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
                             String scope) {
    return authCache.isSuperUser(new Scope(scope), userPrincipal(sessionPrincipal), groupPrincipals);
  }

  @Override
  public Set<AccessRule> accessRules(KafkaPrincipal sessionPrincipal,
                                     Set<KafkaPrincipal> groupPrincipals,
                                     String scope,
                                     Resource resource) {
    return authCache.rbacRules(new Scope(scope),
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
    ClientUtils.closeQuietly(metadataServer, "metadataServer", firstException);
    ClientUtils.closeQuietly(authStore, "authStore", firstException);
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
  public AuthCache authCache() {
    return authCache;
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
    metadataServer.configure(metadataServiceConfig.metadataServerConfigs());
    return metadataServer;
  }

  private class RbacAuthorizer extends EmbeddedAuthorizer {
    RbacAuthorizer() {
      configureProviders(Collections.singletonList(RbacProvider.this), RbacProvider.this, null);
    }
  }

  private static class DummyMetadataServer implements MetadataServer {

    @Override
    public void start(Authorizer embeddedAuthorizer, AuthStore authStore) {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void close() throws IOException {
    }
  }
}
