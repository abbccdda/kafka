package io.confluent.kafka.multitenant.metrics;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.confluent.kafka.multitenant.metrics.TenantMetrics.TENANT_TAG;
import static io.confluent.kafka.multitenant.metrics.TenantMetrics.USER_TAG;

public abstract class AbstractSensorCreator {
  public static final long EXPIRY_SECONDS = TimeUnit.DAYS.toSeconds(7);

  protected final String name;
  protected final String descriptiveName;

  public AbstractSensorCreator(String name, String descriptiveName) {
    this.name = name;
    this.descriptiveName = descriptiveName;
  }

  protected Sensor createSensor(Metrics metrics, String sensorName) {
    return metrics.sensor(sensorName, metrics.config(), EXPIRY_SECONDS);
  }

  protected Sensor createSensor(Metrics metrics, String sensorName, long expirySeconds) {
    return metrics.sensor(sensorName, metrics.config(), expirySeconds);
  }

  protected Meter createMeter(Metrics metrics, String groupName, Map<String, String> metricTags,
                              String baseName, String descriptiveName) {
    MetricName rateMetricName = metrics.metricName(baseName + "-rate", groupName,
            String.format("The number of %s per second", descriptiveName), metricTags);
    MetricName totalMetricName = metrics.metricName(baseName + "-total", groupName,
            String.format("The total number of %s", descriptiveName), metricTags);
    return new Meter(rateMetricName, totalMetricName);
  }

  Map<String, String> tenantTags(MultiTenantPrincipal principal) {
    Map<String, String> tags = new HashMap<>();
    tags.put(TENANT_TAG, principal.tenantMetadata().tenantName);
    tags.put(USER_TAG, principal.user());
    return tags;
  }
}
