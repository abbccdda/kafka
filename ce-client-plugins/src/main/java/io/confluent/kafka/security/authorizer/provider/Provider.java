// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.kafka.security.authorizer.provider;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.common.Configurable;

/**
 * Interface used by providers of user to group mapping used for authorization.
 */
public interface Provider extends Configurable, Closeable {

  /**
   * Starts a provider and returns a future that is completed when the provider is ready.
   * By default, a provider is auto-started on configure and this method returns a completed future.
   * Providers that need to delay startup due to bootstrapping limitations must override this method
   * and return a future that is completed when the provider has started up. This allows brokers to
   * use providers that load metadata from broker topics using the inter-broker listener. External
   * listeners that rely on this metadata will be started when the returned future completes.
   */
  default CompletionStage<Void> start() {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Returns the name of this provider.
   * @return provider name
   */
  String providerName();

  /**
   * Returns true if this provider uses metadata from a Kafka topic on this cluster.
   */
  boolean usesMetadataFromThisKafkaCluster();
}
