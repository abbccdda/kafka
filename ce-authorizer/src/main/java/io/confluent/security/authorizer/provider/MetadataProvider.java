// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import java.util.Map;
import org.apache.kafka.server.http.MetadataServer;

/**
 * Interface used by providers of metadata, e.g. an Embedded Metadata Server.
 */
public interface MetadataProvider extends Provider {
  /**
   * Returns true if minimal configs of this provider are included in the provided configs.
   */
  boolean providerConfigured(Map<String, ?> configs);

  /**
   * Allows this {@link MetadataProvider} to register itself in the given {@link MetadataServer}.
   */
  void registerMetadataServer(MetadataServer metadataServer);
}
