// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant;

import javax.security.sasl.SaslServer;

public interface MultiTenantSaslServer extends SaslServer {
  TenantMetadata tenantMetadata();

  String userIdentifier();
}
