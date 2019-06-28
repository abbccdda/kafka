// (Copyright) [2018 - 2018] Confluent, Inc.

package org.apache.kafka.server.multitenant;

import org.apache.kafka.common.Configurable;

public interface MultiTenantMetadata extends Configurable {

  /**
   * This is called when multitenant metadata watcher is closed
   */
  public void close(String brokerSessionUuid);

  /**
   * This method is called after socket server is started
   * @param endpoint the interbroker endpoint
   */
  public void handleSocketServerInitialized(String endpoint);
}
