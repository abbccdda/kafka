// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.metrics;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;

public class TenantMetrics {
  public static final String TENANT_TAG = "tenant";
  static final String USER_TAG = "user";
  static final String CLIENT_ID_TAG = "client-id";
  static final String GROUP = "tenant-metrics";

  public static class MetricsRequestContext {

    private final MultiTenantPrincipal principal;
    private final String clientId;
    private final ApiKeys apiKey;

    public MetricsRequestContext(MultiTenantPrincipal principal, String clientId, ApiKeys apiKey) {
      this.principal = principal;
      this.clientId = clientId;
      this.apiKey = apiKey;
    }

    public MultiTenantPrincipal principal() {
      return principal;
    }

    public String clientId() {
      return clientId;
    }

    public ApiKeys apiKey() {
      return apiKey;
    }
  }

  private EnumMap<ApiKeys, ApiSensors> apiSensors = new EnumMap<>(ApiKeys.class);
  private ConnectionSensors connectionSensors;
  private PartitionSensors partitionSensors;

  public void recordAuthenticatedConnection(Metrics metrics, MultiTenantPrincipal principal) {
    if (connectionSensors == null) {
      connectionSensors = new ConnectionSensorBuilder(metrics, principal).build();
      connectionSensors.recordAuthenticatedConnection();
    }
  }

  public void recordAuthenticatedDisconnection() {
    if (connectionSensors != null) {
      connectionSensors.recordAuthenticatedDisconnection();
      connectionSensors = null;
    }
  }

  public void recordRequest(Metrics metrics, MetricsRequestContext context, long requestSize, long currentTimeMs) {
    ApiSensors sensors = apiSensors(metrics, context);
    sensors.recordRequest(requestSize, currentTimeMs);
  }

  public void recordResponse(Metrics metrics, MetricsRequestContext context,
      long responseSize, long responseTimeNanos, Map<Errors, Integer> errorCounts, long currentTimeMs) {

    MultiTenantPrincipal principal = context.principal();
    ApiKeys apiKey = context.apiKey();

    ApiSensors sensors = apiSensors(metrics, context);
    Set<Errors> newErrors = sensors.errorsWithoutSensors(errorCounts.keySet());
    if (!newErrors.isEmpty()) {
      ApiSensorBuilder builder = new ApiSensorBuilder(metrics, principal, apiKey);
      builder.addErrorSensors(sensors, newErrors);
    }

    sensors.recordResponse(responseSize, responseTimeNanos, currentTimeMs);
    sensors.recordErrors(errorCounts, currentTimeMs);
  }

  public void recordPartitionStatsIn(Metrics metrics,
                                     MetricsRequestContext context,
                                     TopicPartition topicPartition,
                                     int size,
                                     int numRecords,
                                     long timeMs) {
    partitionSensors(metrics, context).recordStatsIn(topicPartition, size, numRecords, timeMs);
  }


  public void recordPartitionStatsOut(Metrics metrics,
                                      MetricsRequestContext context,
                                      TopicPartition topicPartition,
                                      int size,
                                      int numRecords,
                                      long timeMs) {
    partitionSensors(metrics, context).recordStatsOut(topicPartition, size, numRecords, timeMs);

  }

  private ApiSensors apiSensors(Metrics metrics, MetricsRequestContext context) {
    ApiKeys apiKey = context.apiKey();
    ApiSensors sensors = apiSensors.get(apiKey);
    if (sensors == null) {
      sensors = new ApiSensorBuilder(metrics, context.principal(), apiKey).build();
      apiSensors.put(apiKey, sensors);
    }
    return sensors;
  }

  private PartitionSensors partitionSensors(Metrics metrics, MetricsRequestContext context) {
    if (partitionSensors == null) {
      partitionSensors = new PartitionSensorBuilder(metrics, context).build();
    }
    return partitionSensors;
  }
}
