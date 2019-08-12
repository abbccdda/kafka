// (Copyright) [2017 - 2017] Confluent, Inc.

package io.confluent.kafka.multitenant.metrics;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.protocol.Errors;

public class ApiSensors {
  private final Sensor requestRate;
  private final Sensor requestByteRate;
  private final Sensor responseByteRate;
  private final Sensor responseTime;
  private final EnumMap<Errors, Sensor> errorRates;

  public ApiSensors(Sensor requestRate,
          Sensor requestByteRate,
          Sensor responseByteRate,
          Sensor responseTime) {
    this.requestRate = requestRate;
    this.requestByteRate = requestByteRate;
    this.responseByteRate = responseByteRate;
    this.responseTime = responseTime;
    this.errorRates = new EnumMap<>(Errors.class);
  }

  public void recordRequest(long requestSize, long currentTimeMs) {
    requestRate.record(1.0, currentTimeMs);
    requestByteRate.record(requestSize, currentTimeMs);
  }

  public void recordResponse(long responseSize, long responseTimeNanos, long currentTimeMs) {
    responseByteRate.record(responseSize, currentTimeMs);
    responseTime.record(responseTimeNanos, currentTimeMs);
  }

  public void recordErrors(Map<Errors, Integer> errorCounts, long currentTimeMs) {
    for (Map.Entry<Errors, Integer> entry : errorCounts.entrySet()) {
      errorRates.get(entry.getKey()).record(entry.getValue(), currentTimeMs);
    }
  }

  Set<Errors> errorsWithoutSensors(Set<Errors> errors) {
    Set<Errors> newErrors = new HashSet<>(errors);
    newErrors.removeAll(errorRates.keySet());
    return newErrors;
  }

  void addErrorSensors(Map<Errors, Sensor> errorSensors) {
    this.errorRates.putAll(errorSensors);
  }
}
