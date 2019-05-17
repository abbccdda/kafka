// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.license.validator;

import io.confluent.license.InvalidLicenseException;
import java.io.Closeable;
import org.apache.kafka.common.Configurable;

/**
 * License validator interface used by Kafka brokers for proprietary features. This includes license
 * metrics and periodic error logging for expired license. Implementations may support Kafka-topic
 * based license management or legacy ZooKeeper-based license management.
 */
public interface LicenseValidator extends Configurable, Closeable {

  /**
   * Initialize license using the provided license or free-tier/trial license.
   * @param license License string which may be empty
   * @param metricGroup Metrics group for license status
   * @param componentId Component id included in client-ids of Kafka clients used by license manager
   * @throws InvalidLicenseException if license is invalid or has expired
   */
  void initializeAndVerify(String license, String metricGroup, String componentId) throws InvalidLicenseException;

  /**
   * Verifies the registered license
   * @return true if license is still valid
   */
  boolean verifyLicense();
}
