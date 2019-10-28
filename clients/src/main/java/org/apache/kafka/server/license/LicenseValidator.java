// (Copyright) [2018 - 2019] Confluent, Inc.

package org.apache.kafka.server.license;

import java.io.Closeable;
import org.apache.kafka.common.Configurable;

/**
 * License validator interface used by Kafka brokers for proprietary features. This includes license
 * metrics and periodic error logging for expired license. Implementations may support Kafka-topic
 * based license management or legacy ZooKeeper-based license management.
 */
public interface LicenseValidator extends Configurable, Closeable {

  default boolean enabled() {
    return true;
  }

  /**
   * Initialize license using the provided license or free-tier/trial license and start
   * license validator. If license expires, a message is logged periodically.
   *
   * @param componentId Component id included in client-ids of Kafka clients used by license manager
   */
  void start(String componentId);

  /**
   * Verifies if the registered license is still valid.
   * @return true if license is still valid
   */
  boolean isLicenseValid();
}
