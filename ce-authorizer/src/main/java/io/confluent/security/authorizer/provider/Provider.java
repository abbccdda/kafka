// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import java.io.Closeable;
import java.util.Map;
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
   *
   * @param interBrokerListenerConfigs Client configs for using inter-broker listener
   *    For brokers that host metadata service, these client configs may be used to access metadata
   *    topic if metadata client configs are not explicitly overridden. This avoids the need for
   *    redundant configs for brokers in the metadata cluster.
   */
  default CompletionStage<Void> start(Map<String, ?> interBrokerListenerConfigs) {
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

  /**
   * Returns true if this is a proprietary provider that requires a license
   */
  default boolean needsLicense() {
    return true;
  }
}
