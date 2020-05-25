// (Copyright) [2019 - 2019] Confluent, Inc.

package org.apache.kafka.server.audit;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.UnaryOperator;
import org.apache.kafka.common.Reconfigurable;
import org.apache.kafka.common.metrics.Metrics;

/**
 * Interface used by audit log provider that logs audit events.
 */
public interface AuditLogProvider extends Reconfigurable, AutoCloseable {

  /**
   * Starts a AuditLogProvider and returns a future that is completed when the AuditLogProvider is ready.
   * By default, a provider is auto-started on configure and this method returns a completed future.
   * Providers that need to delay startup due to bootstrapping limitations must override this method
   * and return a future that is completed when the provider has started up. External listeners that
   * rely on this metadata will be started when the returned future completes.
   *
   * @param configs Any additional configs to start the audit log provider.
   */
  default CompletionStage<Void> start(Map<String, ?> configs) {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Returns true if minimal configs of this provider are included in the provided configs.
   */
  boolean providerConfigured(Map<String, ?> configs);

  /**
   * Persists the given audit event to the event store
   *
   * @param auditEvent
   * the event to persist, cannot be <code>null</code>
   */
  void logEvent(AuditEvent auditEvent);

  /**
   * Specifies a transformer that should be applied to the data if we decide to log it.
   * This can be used to remove sensitive or internal data so that it does not appear
   * in the logs.
   * @param sanitizer audit log sanitizer implementation.
   */
  void setSanitizer(UnaryOperator<AuditEvent> sanitizer);

  /**
   * Returns true if this audit log destination topics are in this cluster
   */
  boolean usesMetadataFromThisKafkaCluster();

  /**
   * Set the Kafka Metrics to this audit log provider.
   */
  default void setMetrics(Metrics metrics) {
    return;
  }
}
