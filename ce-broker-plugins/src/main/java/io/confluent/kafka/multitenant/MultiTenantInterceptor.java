// (Copyright) [2017 - 2020] Confluent, Inc.

package io.confluent.kafka.multitenant;

import io.confluent.kafka.multitenant.quota.TenantPartitionAssignor;
import io.confluent.kafka.multitenant.metrics.TenantMetrics;

import io.confluent.kafka.multitenant.quota.TenantQuotaCallback;
import kafka.server.KafkaConfig$;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.interceptor.BrokerInterceptor;

import java.net.InetAddress;
import java.util.Map;

public class MultiTenantInterceptor implements BrokerInterceptor {

  private final Time time;
  private final TenantMetrics tenantMetrics;
  private TenantPartitionAssignor partitionAssignor;
  private short defaultReplicationFactor;
  private int defaultNumPartitions;

  public MultiTenantInterceptor() {
    this.time = Time.SYSTEM;
    this.tenantMetrics = new TenantMetrics();
  }

  @Override
  public void onAuthenticatedConnection(String connectionId, InetAddress clientAddress,
                                        KafkaPrincipal principal, Metrics metrics) {
    if (principal instanceof MultiTenantPrincipal) {
      tenantMetrics.recordAuthenticatedConnection(metrics, (MultiTenantPrincipal) principal);
    } else {
      throw new IllegalStateException("Not a tenant connection");
    }
  }

  @Override
  public void onAuthenticatedDisconnection(String connectionId, InetAddress clientAddress,
                                           KafkaPrincipal principal, Metrics metrics) {
    tenantMetrics.recordAuthenticatedDisconnection();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.partitionAssignor = TenantQuotaCallback.partitionAssignor(configs);
    this.defaultReplicationFactor = (short) intConfig(configs,
        KafkaConfig$.MODULE$.DefaultReplicationFactorProp());
    this.defaultNumPartitions = intConfig(configs,
        KafkaConfig$.MODULE$.NumPartitionsProp());
  }

  @Override
  public RequestContext newContext(RequestHeader header,
                                   String connectionId,
                                   InetAddress clientAddress,
                                   KafkaPrincipal principal,
                                   ListenerName listenerName,
                                   SecurityProtocol securityProtocol,
                                   ClientInformation clientInformation,
                                   Metrics metrics) {
    return new MultiTenantRequestContext(header, connectionId, clientAddress, principal,
        listenerName, securityProtocol, clientInformation, time, metrics, tenantMetrics,
        partitionAssignor, defaultReplicationFactor, defaultNumPartitions);
  }

  private static int intConfig(Map<String, ?> configs, String configName) {
    Object configValue = configs.get(configName);
    if (configValue == null) {
      throw new ConfigException(configName + " is not set");
    }
    return Integer.parseInt(configValue.toString());
  }
}
