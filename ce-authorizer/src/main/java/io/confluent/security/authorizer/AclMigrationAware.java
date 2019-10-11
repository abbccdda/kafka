// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.authorizer;

import org.apache.kafka.server.authorizer.Authorizer;

/**
 * Providers which supports Acl migration from source acl authorizer should implement
 * this interface.
 */
public interface AclMigrationAware {

  /**
   * Returns a runnable task which does Acl migration from a given source authorizer
   * @param sourceAuthorizer source Authorizer from which will migrate the Acls
   * @return Runnable task
   */
  Runnable migrationTask(Authorizer sourceAuthorizer);
}
