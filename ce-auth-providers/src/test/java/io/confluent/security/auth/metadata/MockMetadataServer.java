// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import io.confluent.security.authorizer.ConfluentAuthorizerConfig;
import java.util.Map;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.server.http.MetadataServer;

public class MockMetadataServer implements MetadataServer, ClusterResourceListener {

  public enum ServerState {
    CREATED,
    CONFIGURED,
    REGISTERED,
    STARTED,
    CLOSED
  }

  public ServerState serverState;
  private MetadataServer.Injector injector;

  public MockMetadataServer() {
    this.serverState = ServerState.CREATED;
  }

  @Override
  public synchronized void configure(Map<String, ?> configs) {
    if (serverState != ServerState.CREATED) {
      throw new IllegalStateException("configure() invoked in invalid state: " + serverState);
    }
    serverState = ServerState.CONFIGURED;
  }

  @Override
  public boolean providerConfigured(Map<String, ?> configs) {
    return ConfluentAuthorizerConfig.accessRuleProviders(configs).contains("MOCK_RBAC");
  }

  @Override
  public void registerMetadataProvider(String providerName, MetadataServer.Injector injector) {
    this.injector = injector;
    this.serverState = ServerState.REGISTERED;
  }

  @Override
  public synchronized void start() {
    if (serverState != ServerState.CONFIGURED) {
      throw new IllegalStateException("start() invoked in invalid state: " + serverState);
    }
    serverState = ServerState.STARTED;
  }

  @Override
  public synchronized void close() {
    serverState = ServerState.CLOSED;
  }

  @Override
  public void onUpdate(ClusterResource clusterResource) {
    // Do nothing.
  }

  public <T> T getInjectedInstance(Class<T> clazz) {
    return injector.getInstance(clazz);
  }
}
