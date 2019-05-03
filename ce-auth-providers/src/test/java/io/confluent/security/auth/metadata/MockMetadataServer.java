// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import io.confluent.security.authorizer.Authorizer;
import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;

public class MockMetadataServer implements MetadataServer, ClusterResourceListener {
  public enum ServerState {
    CREATED,
    CONFIGURED,
    STARTED,
    CLOSED
  }

  volatile ServerState serverState;
  volatile Authorizer embeddedAuthorizer;
  volatile AuthStore authStore;
  volatile AuthCache authCache;
  volatile Map<String, ?> configs;
  volatile String metadataClusterId;

  public MockMetadataServer() {
    this.serverState = ServerState.CREATED;
  }

  @Override
  public void configure(Map<String, ?> configs) {
    if (serverState != ServerState.CREATED)
      throw new IllegalStateException("configure() invoked in invalid state: " + serverState);
    this.configs = configs;
    serverState = ServerState.CONFIGURED;
  }

  @Override
  public void start(Authorizer embeddedAuthorizer, AuthStore authStore, AuthenticateCallbackHandler callbackHandler) {
    if (serverState != ServerState.CONFIGURED)
      throw new IllegalStateException("start() invoked in invalid state: " + serverState);

    this.embeddedAuthorizer = embeddedAuthorizer;
    this.authStore = authStore;
    this.authCache = authStore.authCache();
    serverState = ServerState.STARTED;
  }

  @Override
  public void close() throws IOException {
    serverState = ServerState.CLOSED;
  }

  @Override
  public String providerName() {
    return "MOCK_RBAC";
  }

  @Override
  public void onUpdate(ClusterResource clusterResource) {
    this.metadataClusterId = clusterResource.clusterId();
  }
}
