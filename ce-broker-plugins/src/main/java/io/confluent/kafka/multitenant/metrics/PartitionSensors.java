// (Copyright) [2018 - 2018] Confluent, Inc.

package io.confluent.kafka.multitenant.metrics;

import io.confluent.kafka.multitenant.metrics.TenantMetrics.MetricsRequestContext;
import io.confluent.kafka.multitenant.schema.TenantContext;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedSum;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static io.confluent.kafka.multitenant.metrics.TenantMetrics.CLIENT_ID_TAG;
import static io.confluent.kafka.multitenant.metrics.TenantMetrics.GROUP;
import static io.confluent.kafka.multitenant.metrics.TenantMetrics.TENANT_TAG;

public class PartitionSensors {
  public static final String TOPIC_TAG = "topic";
  public static final String PARTITION_TAG = "partition";
  private static final long PARTITION_SENSOR_EXPIRY_SECONDS = 3600;

  private final ThroughputSensors in;
  private final ThroughputSensors out;

  public PartitionSensors(
      MetricsRequestContext context,
      Map<String, Sensor> sensors,
      PartitionSensorBuilder sensorBuilder) {

    in = new ThroughputSensors(
        context,
        PartitionSensorBuilder.BYTES_IN,
        PartitionSensorBuilder.RECORDS_IN,
        sensors.get(PartitionSensorBuilder.BYTES_IN),
        sensors.get(PartitionSensorBuilder.BROKER_SENSOR_PREFIX + PartitionSensorBuilder.BYTES_IN),
        sensorBuilder
    );
    out = new ThroughputSensors(
        context,
        PartitionSensorBuilder.BYTES_OUT,
        PartitionSensorBuilder.RECORDS_OUT,
        sensors.get(PartitionSensorBuilder.BYTES_OUT),
        sensors.get(PartitionSensorBuilder.BROKER_SENSOR_PREFIX + PartitionSensorBuilder.BYTES_OUT),
        sensorBuilder
    );
  }

  public void recordStatsIn(TopicPartition tp, long bytes, long numRecords) {
    this.in.record(tp, bytes, numRecords);
  }

  public void recordStatsOut(TopicPartition tp, long bytes, long numRecords) {
    this.out.record(tp, bytes, numRecords);
  }

  /**
   * Class encapsulating bytes-in or bytes-out throughput percentiles sensors for a tenant.
   */
  static class ThroughputSensors {

    private static final Field STATS_FIELD;

    static {
      // TODO: Add accessor for `stats` to Sensor in ce-kafka
      try {
        STATS_FIELD = Sensor.class.getDeclaredField("stats");
        STATS_FIELD.setAccessible(true);
      } catch (Exception e) {
        throw new KafkaException(e);
      }
    }

    private final String bytesMetricName;
    private final String recordsMetricName;
    private final String tenant;
    private final String clientId;
    private final PartitionSensorBuilder partitionSensorBuilder;
    private final TenantThroughputPercentiles tenantThroughputPercentiles;
    private final ThroughputPercentiles brokerThroughputPercentiles;
    private final Map<TopicPartition, PartitionDetailSensors> partitionSensors;

    ThroughputSensors(MetricsRequestContext context,
                      String bytesMetricName,
                      String recordsMetricName,
                      Sensor tenantPercentilesSensor,
                      Sensor brokerPercentilesSensor,
                      PartitionSensorBuilder partitionSensorBuilder) {
      this.bytesMetricName = bytesMetricName;
      this.recordsMetricName = recordsMetricName;
      this.tenant = context.principal().tenantMetadata().tenantName;
      this.clientId = context.clientId();
      this.partitionSensorBuilder = partitionSensorBuilder;
      this.tenantThroughputPercentiles =
          (TenantThroughputPercentiles) percentilesStats(tenantPercentilesSensor);
      this.brokerThroughputPercentiles =
          (ThroughputPercentiles) percentilesStats(brokerPercentilesSensor);
      this.tenantThroughputPercentiles.brokerThroughputPercentiles = brokerThroughputPercentiles;
      this.partitionSensors = new ConcurrentHashMap<>();

    }

    private static Percentiles percentilesStats(Sensor sensor) {
      try {
        List<?> stats = (List<?>) STATS_FIELD.get(sensor);
        if (stats.size() != 1) {
          throw new KafkaException("Unexpected stats for throughput histogram sensor: " + stats);
        }
        return (Percentiles) stats.get(0);
      } catch (Exception e) {
        throw new KafkaException(e);
      }
    }

    void record(TopicPartition tp, long bytes, long numRecords) {
      partitionPercentileSensor(tp).record(bytes);
      partitionDetailSensors(tp).bytesSensor.record(bytes);
      partitionDetailSensors(tp).recordsSensor.record(numRecords);
    }


    private Sensor partitionPercentileSensor(TopicPartition tp) {
      PartitionStat partitionStat = brokerThroughputPercentiles.partitionStats.get(tp);
      if (partitionStat != null) {
        return partitionStat.rateSensor;
      } else {
        String sensorName = String.format("%s:%s-%s:%s-%s:%s-%s", bytesMetricName,
                                          TENANT_TAG, tenant,
                                          TOPIC_TAG, tp.topic(),
                                          PARTITION_TAG, tp.partition());
        PartitionSensorCreator sensorCreator =
            new PartitionSensorCreator(bytesMetricName, bytesMetricName, tenant, tp, brokerThroughputPercentiles);
        Map<String, AbstractSensorCreator> sensorCreators =
            Collections.singletonMap(sensorName, sensorCreator);
        Map<String, String> sensorsToFind = new HashMap<>(1);
        sensorsToFind.put(sensorName, sensorName);
        return partitionSensorBuilder.getOrCreateSensors(sensorsToFind, sensorCreators)
            .get(sensorName);
      }
    }

    private PartitionDetailSensors partitionDetailSensors(TopicPartition tp) {
      PartitionDetailSensors partitionSensor = partitionSensors.get(tp);
      if (partitionSensor != null) {
        return partitionSensor;
      } else {
        String bytesSensorName = String.format("%s:%s-%s:%s-%s:%s-%s,%s-%s", bytesMetricName,
                                               TENANT_TAG, tenant,
                                               CLIENT_ID_TAG, clientId,
                                               TOPIC_TAG, tp.topic(),
                                               PARTITION_TAG, tp.partition());
        String recordsSensorName = String.format("%s:%s-%s:%s-%s:%s-%s,%s-%s", recordsMetricName,
                                               TENANT_TAG, tenant,
                                               CLIENT_ID_TAG, clientId,
                                               TOPIC_TAG, tp.topic(),
                                               PARTITION_TAG, tp.partition());

        PartitionDetailSensorCreator bytesSensorCreator =
            new PartitionDetailSensorCreator(bytesMetricName, bytesMetricName, tenant, clientId, tp);
        PartitionDetailSensorCreator recordsSensorCreator =
            new PartitionDetailSensorCreator(recordsMetricName, recordsMetricName, tenant, clientId, tp);

        Map<String, AbstractSensorCreator> sensorCreators = new HashMap<>(2);
        sensorCreators.put(bytesSensorName, bytesSensorCreator);
        sensorCreators.put(recordsSensorName, recordsSensorCreator);

        Map<String, String> sensorsToFind = new HashMap<>(2);
        sensorsToFind.put(bytesSensorName, bytesSensorName);
        sensorsToFind.put(recordsSensorName, recordsSensorName);

        Map<String, Sensor> s = partitionSensorBuilder.getOrCreateSensors(sensorsToFind, sensorCreators);
        PartitionDetailSensors sensors = new PartitionDetailSensors(s.get(bytesSensorName),
                                                                    s.get(recordsSensorName));

        PartitionDetailSensors existing = partitionSensors.putIfAbsent(tp, sensors);
        if (existing == null) {
          return sensors;
        } else {
          return existing;
        }
      }
    }
  }

  static class PartitionDetailSensors {
    final Sensor bytesSensor;
    final Sensor recordsSensor;

    public PartitionDetailSensors(Sensor bytesSensor, Sensor recordsSensor) {
      this.bytesSensor = bytesSensor;
      this.recordsSensor = recordsSensor;
    }
  }

  /**
   * Throughput percentiles stat that computes values from partition stats.
   */
  static class ThroughputPercentiles extends Percentiles {

    private final Map<TopicPartition, PartitionStat> partitionStats;

    ThroughputPercentiles(int sizeInBytes, double max, BucketSizing bucketing,
        Percentile... percentiles) {
      super(sizeInBytes, max, bucketing, percentiles);
      partitionStats = new ConcurrentHashMap<>();
    }

    protected void purgeObsoleteSamples(MetricConfig config, long now) {
      purgeObsoleteStats(config, now);
      samples.forEach(sample -> sample.reset(now));
      partitionStats.values().forEach(stat -> {
        double rate = stat.rateStat.measure(config, now);
        record(config, rate, now);
      });
    }

    protected void purgeObsoleteStats(MetricConfig config, long now) {
      partitionStats.values().forEach(stat -> stat.purgeObsoleteSamples(config, now));
      partitionStats.entrySet().removeIf(e -> e.getValue().hasNoEvents());
    }

    Rate partitionStat(TopicPartition tp, Sensor sensor) {
      PartitionStat newStat = new PartitionStat(sensor);
      PartitionStat existing = partitionStats.putIfAbsent(tp, newStat);
      if (existing == null) {
        newStat.rateStat = new Rate(newStat);
        return newStat.rateStat;
      } else {
        return existing.rateStat;
      }
    }
  }

  static class TenantThroughputPercentiles extends Percentiles {

    private final String tenantPrefix;
    volatile ThroughputPercentiles brokerThroughputPercentiles;

    TenantThroughputPercentiles(String tenant,
        int sizeInBytes, double max, BucketSizing bucketing,
        Percentile... percentiles) {
      super(sizeInBytes, max, bucketing, percentiles);
      this.tenantPrefix = tenant + TenantContext.DELIMITER;
    }

    protected void purgeObsoleteSamples(MetricConfig config, long now) {
      brokerThroughputPercentiles.purgeObsoleteStats(config, now);
      samples.forEach(sample -> sample.reset(now));
      brokerThroughputPercentiles.partitionStats.entrySet().stream()
          .filter(e -> e.getKey().topic().startsWith(tenantPrefix))
          .forEach(e -> {
            double rate = e.getValue().rateStat.measure(config, now);
            record(config, rate, now);
          });
    }
  }

  /**
   * Sampled total stat for a partition used to track partition byte rates.
   * Sub-class is to enable identifying partitions for which all samples have become obsolete.
   */
  private static class PartitionStat extends WindowedSum {

    final Sensor rateSensor;
    Rate rateStat;

    PartitionStat(Sensor sensor) {
      this.rateSensor = sensor;
    }

    @Override
    protected void purgeObsoleteSamples(MetricConfig config, long now) {
      super.purgeObsoleteSamples(config, now);
    }

    boolean hasNoEvents() {
      return samples.stream().allMatch(s -> s.eventCount == 0);
    }
  }

  /**
   * Sensor creator for partition throughput percentiles
   */
  static class PercentilesSensorCreator extends AbstractSensorCreator {

    private static final int BUCKETS = 1024;
    private static final double MAX_THROUGHPUT = 1024 * 1024 * 1024;
    private final String tenant;

    PercentilesSensorCreator(Optional<String> tenant, String name) {
      super(name, name);
      this.tenant = tenant.orElse(null);
    }

    @Override
    protected Sensor createSensor(Metrics metrics, String sensorName) {
      Sensor sensor = metrics.sensor(sensorName, metrics.config(), PARTITION_SENSOR_EXPIRY_SECONDS);
      Map<String, String> tags = tenant == null ? Collections.emptyMap()
          : Collections.singletonMap(TENANT_TAG, tenant);
      Percentile[] percentiles = new Percentile[] {
          new Percentile(metrics.metricName(name + "-p25", GROUP, descriptiveName, tags), 25),
          new Percentile(metrics.metricName(name + "-p50", GROUP, descriptiveName, tags), 50),
          new Percentile(metrics.metricName(name + "-p75", GROUP, descriptiveName, tags), 75),
          new Percentile(metrics.metricName(name + "-p95", GROUP, descriptiveName, tags), 95),
          new Percentile(metrics.metricName(name + "-p99", GROUP, descriptiveName, tags), 99),
          new Percentile(metrics.metricName(name + "-p99.9", GROUP, descriptiveName, tags), 99.9)
      };
      if (tenant == null) {
        sensor.add(new ThroughputPercentiles(BUCKETS * 4,
            MAX_THROUGHPUT,
            BucketSizing.LINEAR,
            percentiles));
      } else {
        sensor.add(new TenantThroughputPercentiles(tenant,
            BUCKETS * 4,
            MAX_THROUGHPUT,
            BucketSizing.LINEAR,
            percentiles));
      }
      return sensor;
    }
  }

  /**
   * Sensor creator for throughput rate by partition and tenant
   */
  static class PartitionSensorCreator extends AbstractSensorCreator {

    private final ThroughputPercentiles throughputPercentiles;
    private final TopicPartition tp;
    private final String tenant;

    PartitionSensorCreator(String name, String descriptiveName,
        String tenant, TopicPartition tp, ThroughputPercentiles throughputPercentiles) {
      super(name, descriptiveName);
      this.tenant = tenant;
      this.tp = tp;
      this.throughputPercentiles = throughputPercentiles;
    }

    protected Sensor createSensor(Metrics metrics, String sensorName) {
      Map<String, String> tags = new HashMap<>();
      tags.put(TenantMetrics.TENANT_TAG, tenant);
      tags.put(TOPIC_TAG, tp.topic());
      tags.put(PARTITION_TAG, String.valueOf(tp.partition()));
      Sensor sensor = super.createSensor(metrics, sensorName, PARTITION_SENSOR_EXPIRY_SECONDS);
      sensor.add(metrics.metricName(name, GROUP, tags),
          throughputPercentiles.partitionStat(tp, sensor));
      return sensor;
    }
  }

  /**
   * Sensor creator for throughput meter by partition, tenant, and client id
   */
  static class PartitionDetailSensorCreator extends AbstractSensorCreator {

    private final String tenant;
    private final String clientId;
    private final TopicPartition tp;

    PartitionDetailSensorCreator(String name, String descriptiveName, String tenant, String clientId, TopicPartition tp) {
      super(name, descriptiveName);
      this.tenant = tenant;
      this.clientId = clientId;
      this.tp = tp;
    }

    protected Sensor createSensor(Metrics metrics, String sensorName) {
      Sensor sensor = super.createSensor(metrics, sensorName, PARTITION_SENSOR_EXPIRY_SECONDS);

      Map<String, String> tags = new HashMap<>();
      // do not publish partition detail metrics to JMX: exposing a large number of JMX metrics can be resource intensive
      tags.put(JmxReporter.JMX_IGNORE_TAG, "");
      tags.put(TenantMetrics.TENANT_TAG, tenant);
      tags.put(CLIENT_ID_TAG, clientId);
      tags.put(TOPIC_TAG, tp.topic());
      tags.put(PARTITION_TAG, String.valueOf(tp.partition()));

      sensor.add(createMeter(metrics, GROUP, tags, name, name));
      return sensor;
    }
  }
}
