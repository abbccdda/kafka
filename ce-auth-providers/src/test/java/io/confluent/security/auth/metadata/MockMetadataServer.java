// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import java.util.Map;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.server.http.MetadataServer;

public class MockMetadataServer implements MetadataServer, ClusterResourceListener {
  public enum ServerState {
    CREATED,
    CONFIGURED,
    STARTED,
    CLOSED
  }

  private ServerState serverState;

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
  public String serverName() {
    return "MOCK_RBAC";
  }

  @Override
  public void registerMetadataProvider(String providerName, MetadataServer.Injector injector) {
    // Do nothing.
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
}
