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
   * Creates a {@code MetadataServer}.
   */
  public static MetadataServer create(String clusterId, Map<String, ?> configurations) {
    MetadataServerConfig metadataServerConfig = new MetadataServerConfig(configurations);

    if (!metadataServerConfig.isServerEnabled()) {
      log.info("MetadataServer is disabled on this broker");
      return none();
    }

    ServiceLoader<MetadataServer> serviceLoader = ServiceLoader.load(MetadataServer.class);

    ArrayList<MetadataServer> implementations = new ArrayList<>();
    for (MetadataServer implementation : serviceLoader) {
      if (implementation.providerConfigured(configurations)) {
        implementations.add(implementation);
      }
    }

    if (implementations.isEmpty()) {
      log.warn("Could not find suitable MetadataServer implementation.");
      return none();
    }
    if (implementations.size() > 1) {
      throw new ConfigException(
          String.format("Found multiple MetadataServer implementations : %s.", implementations));
    }

    MetadataServer metadataServer = implementations.get(0);
    if (metadataServer instanceof ClusterResourceListener) {
      ((ClusterResourceListener) metadataServer).onUpdate(new ClusterResource(clusterId));
    }
    metadataServer.configure(metadataServerConfig.serverConfigs());
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
    public boolean providerConfigured(Map<String, ?> configs) {
      return true;
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
