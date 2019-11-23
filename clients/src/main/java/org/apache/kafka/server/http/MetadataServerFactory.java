// (Copyright) [2018 - 2019] Confluent, Inc.

package org.apache.kafka.server.http;

import java.util.ArrayList;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MetadataServerFactory {

  protected static final Logger log = LoggerFactory.getLogger(MetadataServerFactory.class);

  /**
   * Creates a {@code MetadataServer} based on {@link
   * MetadataServerConfig#METADATA_SERVER_NAME_PROP} configuration.
   */
  public static MetadataServer create(String clusterId, Map<String, ?> configurations) {
    MetadataServerConfig metadataServerConfig = new MetadataServerConfig(configurations);

    if (!metadataServerConfig.isMetadataServerEnabled()) {
      return none();
    }

    ServiceLoader<MetadataServer> serviceLoader = ServiceLoader.load(MetadataServer.class);

    ArrayList<MetadataServer> implementations = new ArrayList<>();
    for (MetadataServer implementation : serviceLoader) {
      if (implementation.serverName().equals(metadataServerConfig.metadataServerName())) {
        implementations.add(implementation);
      }
    }

    if (implementations.isEmpty()) {
      log.warn(
          "Could not find suitable MetadataServer implementation with name {}.",
          metadataServerConfig.metadataServerName());
      return none();
    }
    if (implementations.size() > 1) {
      throw new ConfigException(
          String.format(
              "Found multiple MetadataServer implementations with name %s.",
              metadataServerConfig.metadataServerName()));
    }

    MetadataServer metadataServer = implementations.get(0);
    if (metadataServer instanceof ClusterResourceListener) {
      ((ClusterResourceListener) metadataServer).onUpdate(new ClusterResource(clusterId));
    }
    metadataServer.configure(metadataServerConfig.metadataServerConfigs());
    return metadataServer;
  }

  public static MetadataServer none() {
    return NoneMetadataServer.INSTANCE;
  }

  private static class NoneMetadataServer implements MetadataServer {

    private static final NoneMetadataServer INSTANCE = new NoneMetadataServer();

    @Override
    public void configure(Map<String, ?> configs) {
      // Do nothing.
    }

    @Override
    public String serverName() {
      return MetadataServers.NONE.name();
    }

    @Override
    public void registerMetadataProvider(String providerName, Injector injector) {
      // Do nothing.
    }

    @Override
    public void start() {
      // Do nothing.
    }

    @Override
    public void close() {
      // Do nothing.
    }
  }
}
