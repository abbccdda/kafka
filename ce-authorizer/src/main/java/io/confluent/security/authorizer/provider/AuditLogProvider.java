// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;

import java.util.Map;
import java.util.function.UnaryOperator;
import org.apache.kafka.common.Reconfigurable;

/**
 * Interface used by audit log provider that logs authorization events.
 */
public interface AuditLogProvider extends Provider, Reconfigurable {

  /**
   * Generates an audit log entry with the provided data.
   */
  void logAuthorization(AuthorizationLogData data);

  /**
   * Returns true if minimal configs of this provider are included in the provided configs.
   */
  boolean providerConfigured(Map<String, ?> configs);

  /**
   * Specifies a transformer that should be applied to the data if we decide to log it.
   * This can be used to remove sensitive or internal data so that it does not appear
   * in the logs.
   * @param sanitizer
   */
  void setSanitizer(UnaryOperator<AuthorizationLogData> sanitizer);
}
