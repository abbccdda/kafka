// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.metrics;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Min;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;

import io.confluent.kafka.multitenant.MultiTenantPrincipal;

import static io.confluent.kafka.multitenant.metrics.TenantMetrics.GROUP;
import static io.confluent.kafka.multitenant.metrics.TenantMetrics.TENANT_TAG;
import static io.confluent.kafka.multitenant.metrics.TenantMetrics.USER_TAG;

public class ApiSensorBuilder extends AbstractSensorBuilder<MultiTenantPrincipal, ApiSensors> {

  public static final long EXPIRY_SECONDS = TimeUnit.DAYS.toSeconds(7);
  private static final String REQUEST_TAG = "request";
  private static final String ERROR_TAG = "error";

  private final ApiKeys apiKey;
  private static final String REQUEST_RATE = "request";
  private static final String REQUEST_BYTE_RATE = "request-byte";
  private static final String RESPONSE_BYTE_RATE = "response-byte";
  private static final String RESPONSE_TIME_NANOS = "response-time-ns";
  private static final String ERROR_COUNT = "error";
  private static final Map<Errors, ErrorCountSensorCreator> ERROR_SENSOR_CREATORS;
  private static final Map<String, AbstractApiSensorCreator> REQUEST_RESPONSE_SENSOR_CREATORS;


  static {
    REQUEST_RESPONSE_SENSOR_CREATORS = new HashMap<>();
    REQUEST_RESPONSE_SENSOR_CREATORS.put(REQUEST_RATE,
        new RequestMeterSensorCreator(REQUEST_RATE, "requests"));
    REQUEST_RESPONSE_SENSOR_CREATORS.put(REQUEST_BYTE_RATE,
        new RequestSensorCreator(REQUEST_BYTE_RATE, "request bytes", true, true));
    REQUEST_RESPONSE_SENSOR_CREATORS.put(RESPONSE_BYTE_RATE,
        new RequestSensorCreator(RESPONSE_BYTE_RATE, "response bytes", true, true));
    REQUEST_RESPONSE_SENSOR_CREATORS.put(RESPONSE_TIME_NANOS,
        new RequestMinMaxAvgSensorCreator(RESPONSE_TIME_NANOS, "request processing time in nanos"));
    ERROR_SENSOR_CREATORS = new EnumMap<>(Errors.class);
    for (Errors error : Errors.values()) {
      ERROR_SENSOR_CREATORS.put(error, new ErrorCountSensorCreator(error));
    }
  }

  public ApiSensorBuilder(Metrics metrics, MultiTenantPrincipal principal, ApiKeys apiKey) {
    super(metrics, principal);
    this.apiKey = apiKey;
  }

  @Override
  public ApiSensors build() {
    Map<String, Sensor> sensors = getOrCreateSuffixedSensors();
    return new ApiSensors(sensors.get(REQUEST_RATE),
        sensors.get(REQUEST_BYTE_RATE),
        sensors.get(RESPONSE_BYTE_RATE),
        sensors.get(RESPONSE_TIME_NANOS));
  }

  @Override
  protected String sensorSuffix(String unused, MultiTenantPrincipal principal) {
    return String.format(":%s-%s:%s-%s:%s-%s", REQUEST_TAG, apiKey.name,
            TENANT_TAG, principal.tenantMetadata().tenantName,
            USER_TAG, principal.user());
  }

  @Override
  protected Map<String, ? extends AbstractSensorCreator> sensorCreators() {
    return ApiSensorBuilder.REQUEST_RESPONSE_SENSOR_CREATORS;
  }

  @Override
  <T> Sensor createSensor(Map<T, ? extends AbstractSensorCreator> sensorCreators,
                          T sensorKey, String sensorName) {
    AbstractApiSensorCreator sensorCreator =
            (AbstractApiSensorCreator) sensorCreators.get(sensorKey);
    return sensorCreator.createSensor(metrics, sensorName, context, apiKey);
  }

  public void addErrorSensors(ApiSensors apiSensors, Set<Errors> errors) {
    apiSensors.addErrorSensors(getOrCreateErrorSensors(errors));
  }

  private Map<Errors, Sensor> getOrCreateErrorSensors(Set<Errors> errors) {
    String sensorSuffix = sensorSuffix("", context);
    Map<Errors, String> sensorsToFind = new HashMap<>();
    for (Errors error: errors) {
      String sensorName = String.format("%s:%s-%s%s", ERROR_COUNT, ERROR_TAG, error.name(),
          sensorSuffix);
      sensorsToFind.put(error, sensorName);
    }
    return getOrCreateSensors(sensorsToFind, ERROR_SENSOR_CREATORS);
  }

  private abstract static class AbstractApiSensorCreator extends AbstractSensorCreator {
    AbstractApiSensorCreator(String name, String descriptiveName) {
      super(name, descriptiveName);
    }

    abstract Sensor createSensor(Metrics metrics, String sensorName, MultiTenantPrincipal principal,
        ApiKeys apiKey);

    Map<String, String> metricTags(MultiTenantPrincipal principal, ApiKeys apiKey) {
      Map<String, String> tags = tenantTags(principal);
      tags.put(REQUEST_TAG, apiKey.name);
      return tags;
    }
  }

  private static class RequestSensorCreator extends AbstractApiSensorCreator {
    private final boolean toCreateMinMaxAvgMetrics;
    private final boolean toCreateMeterMetrics;

    RequestSensorCreator(String name, String descriptiveName, boolean createMinMaxAvgMetrics,
                         boolean createMeterMetrics) {
      super(name, descriptiveName);
      toCreateMinMaxAvgMetrics = createMinMaxAvgMetrics;
      toCreateMeterMetrics = createMeterMetrics;
    }

    Sensor createSensor(Metrics metrics, String sensorName, MultiTenantPrincipal principal,
                        ApiKeys apiKey) {
      Sensor sensor = super.createSensor(metrics, sensorName);
      Map<String, String> metricTags = metricTags(principal, apiKey);

      if (toCreateMeterMetrics) {
        sensor.add(createMeter(metrics, GROUP, metricTags, name, descriptiveName));
      }
      if (toCreateMinMaxAvgMetrics) {
        sensor.add(metrics.metricName(name + "-min", GROUP,
                "The minimum time taken for " + descriptiveName, metricTags),
                new Min());
        sensor.add(metrics.metricName(name + "-avg", GROUP,
                "The average time taken for " + descriptiveName, metricTags),
                new Avg());
        sensor.add(metrics.metricName(name + "-max", GROUP,
                "The maximum time taken for " + descriptiveName, metricTags),
                new Max());
      }

      return sensor;
    }
  }

  private static class RequestMeterSensorCreator extends RequestSensorCreator {
    RequestMeterSensorCreator(String name, String descriptiveName) {
      super(name, descriptiveName, false, true);
    }
  }

  private static class RequestMinMaxAvgSensorCreator extends RequestSensorCreator {
    RequestMinMaxAvgSensorCreator(String name, String descriptiveName) {
      super(name, descriptiveName, true, false);
    }
  }


  private static class ErrorCountSensorCreator extends AbstractApiSensorCreator {
    private final Errors error;

    ErrorCountSensorCreator(Errors error) {
      super(ERROR_COUNT, "errors");
      this.error = error;
    }

    Sensor createSensor(Metrics metrics, String sensorName, MultiTenantPrincipal principal,
        ApiKeys apiKey) {
      Sensor sensor = super.createSensor(metrics, sensorName);
      Map<String, String> metricTags = metricTags(principal, apiKey);
      metricTags.put(ERROR_TAG, error.name());
      sensor.add(createMeter(metrics, GROUP, metricTags, name, descriptiveName));
      return sensor;
    }
  }
}
