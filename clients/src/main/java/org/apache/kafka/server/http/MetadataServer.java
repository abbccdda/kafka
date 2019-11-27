// (Copyright) [2018 - 2019] Confluent, Inc.

package org.apache.kafka.server.http;

import java.io.Closeable;
import java.util.Map;
import org.apache.kafka.common.Configurable;

public interface MetadataServer extends Configurable, Closeable {

  /**
   * Returns true if minimal configs for this metadata server have been provided.
   */
  boolean providerConfigured(Map<String, ?> configs);

  /**
   * Registers a provider with this {@code MetadataServer}.
   */
  void registerMetadataProvider(String providerName, Injector injector);

  /**
   * Starts the {@code MetadataServer}.
   *
   * <p>This method should return as soon as possible after initiating server start since the
   * broker startup will be blocked until this method returns.
   */
  void start();

  /**
   * An injector to inject runtime dependencies from a {@code
   * io.confluent.security.authorizer.provider.MetadataProvider} into this {@code MetadataService}.
   */
  interface Injector {

    /**
     * Returns the instance bound to the given {@code clazz}.
     */
    <T> T getInstance(Class<T> clazz);
  }
}
