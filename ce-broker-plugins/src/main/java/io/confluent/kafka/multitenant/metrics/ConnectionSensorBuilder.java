// (Copyright) [2017 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.metrics;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Total;

import java.util.HashMap;
import java.util.Map;

import static io.confluent.kafka.multitenant.metrics.TenantMetrics.GROUP;
import static io.confluent.kafka.multitenant.metrics.TenantMetrics.TENANT_TAG;
import static io.confluent.kafka.multitenant.metrics.TenantMetrics.USER_TAG;

public class ConnectionSensorBuilder extends AbstractSensorBuilder<MultiTenantPrincipal, ConnectionSensors> {
  private static final String AUTH_RATE = "successful-authentication";
  private static final String AUTH_CONNECTION_COUNT = "active-authenticated-connection";
  private static final Map<String, AbstractSensorCreator> CONNECTION_SENSOR_CREATORS;

  static {
    CONNECTION_SENSOR_CREATORS = new HashMap<>();
    CONNECTION_SENSOR_CREATORS.put(AUTH_RATE,
            new ConnectionMeterSensorCreator(AUTH_RATE, "successful-authentications"));
    CONNECTION_SENSOR_CREATORS.put(AUTH_CONNECTION_COUNT,
            new ConnectionCountSensorCreator(AUTH_CONNECTION_COUNT,
                    "active-authenticated-connections"));
  }


  public ConnectionSensorBuilder(Metrics metrics, MultiTenantPrincipal principal) {
    super(metrics, principal);
  }

  @Override
  public ConnectionSensors build() {
    Map<String, Sensor> sensors = getOrCreateSuffixedSensors();
    return new ConnectionSensors(sensors.get(AUTH_RATE),
            sensors.get(AUTH_CONNECTION_COUNT)
    );
  }

  @Override
  <T> Sensor createSensor(Map<T, ? extends AbstractSensorCreator> sensorCreators,
                                   T sensorKey, String sensorName) {
    AbstractConnectionSensorCreator sensorCreator =
            (AbstractConnectionSensorCreator) sensorCreators.get(sensorKey);
    return sensorCreator.createSensor(metrics, sensorName, context);
  }

  @Override
  protected String sensorSuffix(String unused, MultiTenantPrincipal principal) {
    return String.format(":%s-%s:%s-%s",
            TENANT_TAG, principal.tenantMetadata().tenantName,
            USER_TAG, principal.user());
  }

  @Override
  protected Map<String, ? extends AbstractSensorCreator> sensorCreators() {
    return ConnectionSensorBuilder.CONNECTION_SENSOR_CREATORS;
  }

  abstract static class AbstractConnectionSensorCreator extends AbstractSensorCreator {
    AbstractConnectionSensorCreator(String name, String descriptiveName) {
      super(name, descriptiveName);
    }

    abstract Sensor createSensor(Metrics metrics, String sensorName,
                                 MultiTenantPrincipal principal);
  }

  private static class ConnectionMeterSensorCreator extends AbstractConnectionSensorCreator {
    ConnectionMeterSensorCreator(String name, String descriptiveName) {
      super(name, descriptiveName);
    }

    Sensor createSensor(Metrics metrics, String sensorName, MultiTenantPrincipal principal) {
      Sensor sensor = super.createSensor(metrics, sensorName);
      Map<String, String> metricTags = tenantTags(principal);

      sensor.add(createMeter(metrics, GROUP, metricTags, name, descriptiveName));

      return sensor;
    }
  }

  private static class ConnectionCountSensorCreator extends AbstractConnectionSensorCreator {
    ConnectionCountSensorCreator(String name, String descriptiveName) {
      super(name, descriptiveName);
    }

    /**
     * Creates a {@link Sensor} with a {@link Total} stat
     *  which is incremented/decremented whenever a connection is established/dropped respectively
     */
    Sensor createSensor(Metrics metrics, String sensorName, MultiTenantPrincipal principal) {
      Sensor sensor = super.createSensor(metrics, sensorName);
      Map<String, String> metricTags = tenantTags(principal);
      MetricName countMetricName = metrics.metricName(name + "-count", GROUP,
              String.format("The current number of %s", descriptiveName), metricTags);
      sensor.add(countMetricName, new Total());

      return sensor;
    }
  }
}
