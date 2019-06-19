package io.confluent.kafka.multitenant.metrics;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public abstract class AbstractSensorBuilder<C, S> {
  private static final ReadWriteLock LOCK = new ReentrantReadWriteLock();

  protected final Metrics metrics;
  protected final C context;

  /**
   * @param metrics the metrics registry to
   * @param context the context in which to create the metrics
   *                used to uniquely identify sensors and metrics across different contexts
   *                (e.g. for tenant level metrics, this could be a MultitenantPrincipal)
   */
  public AbstractSensorBuilder(Metrics metrics, C context) {
    this.metrics = metrics;
    this.context = context;
  }

  /**
   * Returns an object which provides a way to record values in the sensors
   */
  public abstract S build();

  protected abstract String sensorSuffix(String name, C context);

  protected abstract Map<String, ? extends AbstractSensorCreator> sensorCreators();

  Map<String, Sensor> getOrCreateSuffixedSensors() {
    Map<String, String> sensorsToFind = new HashMap<>();
    for (String name : sensorCreators().keySet()) {
      sensorsToFind.put(name, name + sensorSuffix(name, context));
    }
    return getOrCreateSensors(sensorsToFind, sensorCreators());
  }

  <T> Map<T, Sensor> getOrCreateSensors(Map<T, String> sensorsToFind,
                                        Map<T, ? extends AbstractSensorCreator> sensorCreators) {
    Map<T, Sensor> sensors;
    LOCK.readLock().lock();
    try {
      sensors = findSensors(metrics, sensorsToFind);
      sensorsToFind.keySet().removeAll(sensors.keySet());
    } finally {
      LOCK.readLock().unlock();
    }

    if (!sensorsToFind.isEmpty()) {
      LOCK.writeLock().lock();
      try {
        Map<T, Sensor> existingSensors = findSensors(metrics, sensorsToFind);
        sensorsToFind.keySet().removeAll(existingSensors.keySet());
        sensors.putAll(existingSensors);

        for (Map.Entry<T, String> entry : sensorsToFind.entrySet()) {
          T key = entry.getKey();
          String sensorName = entry.getValue();
          sensors.put(key, createSensor(sensorCreators, key, sensorName));
        }
      } finally {
        LOCK.writeLock().unlock();
      }
    }
    return sensors;
  }

  abstract <T> Sensor createSensor(Map<T, ? extends AbstractSensorCreator> sensorCreators,
                          T sensorKey, String sensorName);

  private <T> Map<T, Sensor> findSensors(Metrics metrics, Map<T, String> sensorNames) {
    Map<T, Sensor> existingSensors = new HashMap<>();
    for (Map.Entry<T, String> entry : sensorNames.entrySet()) {
      Sensor sensor = metrics.getSensor(entry.getValue());
      if (sensor != null) {
        existingSensors.put(entry.getKey(), sensor);
      }
    }
    return existingSensors;
  }
}
