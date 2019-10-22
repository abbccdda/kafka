// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.authorizer.provider;


/**
 * Interface used by providers which perform audit and require an audit log provider.
 * This is used to ensure that a single audit log provider can be shared across all
 * providers that perform audit.
 */
public interface Auditable {

  /**
   * Sets audit log provider for an auditable provider.
   * @param auditLogProvider Audit log provider
   */
  void auditLogProvider(AuditLogProvider auditLogProvider);
}
