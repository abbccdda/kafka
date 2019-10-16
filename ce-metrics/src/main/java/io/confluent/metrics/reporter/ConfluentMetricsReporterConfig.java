// (Copyright) [2016 - 2016] Confluent, Inc.

package io.confluent.metrics.reporter;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import io.confluent.monitoring.common.MonitoringProducerDefaults;
import io.confluent.monitoring.common.TimeBucket;

public class ConfluentMetricsReporterConfig extends AbstractConfig {

  public static final String METRICS_REPORTER_PREFIX = "confluent.metrics.reporter.";

  private static final ConfigDef CONFIG;

  public static final String BOOTSTRAP_SERVERS_CONFIG =
      METRICS_REPORTER_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
  public static final String BOOTSTRAP_SERVERS_DOC = "Bootstrap servers for the Kafka cluster "
      + "metrics will be published to. The metrics cluster may be different from the cluster(s) "
      + "whose metrics are being collected. Several production Kafka clusters can publish to a "
      + "single metrics cluster, for example.";

  // metrics topic default settings should be consistent with
  // control center default metrics topic settings
  // to ensure topics have the same settings regardless of which one starts first
  public static final String TOPIC_CONFIG = METRICS_REPORTER_PREFIX + "topic";
  public static final String DEFAULT_TOPIC_CONFIG = "_confluent-metrics";
  public static final String TOPIC_DOC = "Topic on which metrics data will be written.";

  public static final String TOPIC_CREATE_CONFIG = METRICS_REPORTER_PREFIX + "topic.create";
  public static final boolean DEFAULT_TOPIC_CREATE_CONFIG = true;
  public static final String TOPIC_CREATE_DOC = "Create the metrics topic if it does not exist.";

  public static final String TOPIC_PARTITIONS_CONFIG = METRICS_REPORTER_PREFIX + "topic.partitions";
  public static final int DEFAULT_TOPIC_PARTITIONS_CONFIG = 12;
  public static final String TOPIC_PARTITIONS_DOC = "Number of partitions in the metrics topic.";

  public static final String TOPIC_REPLICAS_CONFIG = METRICS_REPORTER_PREFIX + "topic.replicas";
  public static final int DEFAULT_TOPIC_REPLICAS_CONFIG = 3;
  public static final String TOPIC_REPLICAS_DOC =
      "Number of replicas in the metric topic. It must not be higher than the number "
      + "of brokers in the Kafka cluster.";
  // set minIsr to be consistent with
  // control center {@link io.confluent.controlcenter.ControlCenterConfig.CONTROL_CENTER_INTERNAL_TOPIC_DEFAULT_MIN_ISR}
  public static final int DEFAULT_MIN_ISR = 1;

  public static final String TOPIC_RETENTION_MS_CONFIG =
      METRICS_REPORTER_PREFIX + "topic.retention.ms";
  public static final long DEFAULT_TOPIC_RETENTION_MS_CONFIG = TimeUnit.DAYS.toMillis(3);
  public static final String TOPIC_RETENTION_MS_DOC = "Retention time for the metrics topic.";

  public static final String TOPIC_RETENTION_BYTES_CONFIG =
      METRICS_REPORTER_PREFIX + "topic.retention.bytes";
  public static final long DEFAULT_TOPIC_RETENTION_BYTES_CONFIG = -1L;
  public static final String TOPIC_RETENTION_BYTES_DOC = "Retention bytes for the metrics topic.";

  public static final String TOPIC_ROLL_MS_CONFIG = METRICS_REPORTER_PREFIX + "topic.roll.ms";
  public static final long DEFAULT_TOPIC_ROLL_MS_CONFIG = TimeUnit.HOURS.toMillis(4);
  public static final String TOPIC_ROLL_MS_DOC = "Log rolling time for the metrics topic.";

  public static final String TOPIC_MAX_MESSAGE_BYTES_CONFIG =
      METRICS_REPORTER_PREFIX + "topic." + TopicConfig.MAX_MESSAGE_BYTES_CONFIG;
  public static final int DEFAULT_TOPIC_MAX_MESSAGE_BYTES_CONFIG =
      MonitoringProducerDefaults.MAX_REQUEST_SIZE;
  public static final String
      TOPIC_MAX_MESSAGE_BYTES_DOC = "Maximum message size for the metrics topic.";

  public static final String PUBLISH_PERIOD_CONFIG = METRICS_REPORTER_PREFIX + "publish.ms";
  public static final Long DEFAULT_PUBLISH_PERIOD = TimeBucket.SIZE;
  public static final String PUBLISH_PERIOD_DOC = "The metrics reporter will publish new metrics "
        + "to the metrics topic in intervals defined by this setting. This means that control "
        + "center system health data lags by this duration, or that rebalancer may compute a plan "
        + "based on broker data that is stale by this duration. The default is a reasonable value "
        + "for production environments and it typically does not need to be changed.";

  public static final String WHITELIST_CONFIG = METRICS_REPORTER_PREFIX + "whitelist";

  public static final List<String> DEFAULT_BROKER_MONITORING_METRICS = Collections.unmodifiableList(
      Arrays.asList(
          "ActiveControllerCount",
          "BytesInPerSec",
          "BytesOutPerSec",
          "CaughtUpReplicasCount",
          "FailedFetchRequestsPerSec",
          "FailedProduceRequestsPerSec",
          "InSyncReplicasCount",
          "LeaderCount",
          "LeaderElectionRateAndTimeMs",
          "LocalTimeMs",
          "LogEndOffset",
          "LogStartOffset",
          "NetworkProcessorAvgIdlePercent",
          "NumLogSegments",
          "OfflinePartitionsCount",
          "PartitionCount",
          "RemoteTimeMs",
          "ReplicasCount",
          "RequestHandlerAvgIdlePercent",
          "RequestQueueSize",
          "RequestQueueTimeMs",
          "RequestsPerSec",
          "ResponseQueueSize",
          "ResponseQueueTimeMs",
          "ResponseSendTimeMs",
          "Size",
          "TotalFetchRequestsPerSec",
          "TotalProduceRequestsPerSec",
          "TotalTimeMs",
          "UncleanLeaderElectionsPerSec",
          "UnderReplicated",
          "UnderReplicatedPartitions",
          "ZooKeeperDisconnectsPerSec",
          "ZooKeeperExpiresPerSec"
      )
  );

  public static final String DEFAULT_WHITELIST;

  static {
    StringBuilder builder = new StringBuilder(
        ".*MaxLag.*|kafka.log:type=Log,name=Size.*"
    );
    Iterator<String> it = DEFAULT_BROKER_MONITORING_METRICS.iterator();
    if (it.hasNext()) {
      builder.append("|.*name=(");
      builder.append(it.next());
      while (it.hasNext()) {
        builder.append("|");
        builder.append(it.next());
      }
      builder.append(").*");
    }
    DEFAULT_WHITELIST = builder.toString();
  }

  public static final String WHITELIST_DOC =
      "Regex matching the yammer metric mbean name or Kafka metric name to be published to the "
      + "metrics topic.\n\nBy default this includes all the metrics required by Confluent "
      + "Control Center and Confluent Auto Data Balancer. This should typically never be "
      + "modified unless requested by Confluent.";

  public static final String
      VOLUME_METRICS_REFRESH_PERIOD_MS =
      METRICS_REPORTER_PREFIX + "volume.metrics.refresh.ms";
  public static final long DEFAULT_VOLUME_METRICS_REFRESH_PERIOD = 15000L;
  public static final String VOLUME_METRICS_REFRESH_PERIOD_DOC =
      "The minimum interval at which to fetch new volume metrics.";

  static {
    CONFIG = new ConfigDef()
        .define(
            BOOTSTRAP_SERVERS_CONFIG,
            ConfigDef.Type.STRING,
            ConfigDef.Importance.HIGH,
            BOOTSTRAP_SERVERS_DOC
        ).define(
            TOPIC_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_TOPIC_CONFIG,
            ConfigDef.Importance.LOW,
            TOPIC_DOC
        ).define(
            TOPIC_CREATE_CONFIG,
            ConfigDef.Type.BOOLEAN,
            DEFAULT_TOPIC_CREATE_CONFIG,
            ConfigDef.Importance.LOW,
            TOPIC_CREATE_DOC
        ).define(
            TOPIC_PARTITIONS_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_TOPIC_PARTITIONS_CONFIG,
            ConfigDef.Importance.LOW,
            TOPIC_PARTITIONS_DOC
        ).define(
            TOPIC_REPLICAS_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_TOPIC_REPLICAS_CONFIG,
            ConfigDef.Importance.LOW,
            TOPIC_REPLICAS_DOC
        ).define(
            TOPIC_RETENTION_MS_CONFIG,
            ConfigDef.Type.LONG,
            DEFAULT_TOPIC_RETENTION_MS_CONFIG,
            ConfigDef.Importance.LOW,
            TOPIC_RETENTION_MS_DOC
        ).define(
            TOPIC_RETENTION_BYTES_CONFIG,
            ConfigDef.Type.LONG,
            DEFAULT_TOPIC_RETENTION_BYTES_CONFIG,
            ConfigDef.Importance.LOW,
            TOPIC_RETENTION_BYTES_DOC
        ).define(
            TOPIC_ROLL_MS_CONFIG,
            ConfigDef.Type.LONG,
            DEFAULT_TOPIC_ROLL_MS_CONFIG,
            ConfigDef.Importance.LOW,
            TOPIC_ROLL_MS_DOC
        ).define(
            TOPIC_MAX_MESSAGE_BYTES_CONFIG,
            ConfigDef.Type.INT,
            DEFAULT_TOPIC_MAX_MESSAGE_BYTES_CONFIG,
            ConfigDef.Range.atLeast(0),
            ConfigDef.Importance.MEDIUM,
            TOPIC_MAX_MESSAGE_BYTES_DOC
        ).define(
            PUBLISH_PERIOD_CONFIG,
            ConfigDef.Type.LONG,
            DEFAULT_PUBLISH_PERIOD,
            ConfigDef.Importance.LOW,
            PUBLISH_PERIOD_DOC
        ).define(
            WHITELIST_CONFIG,
            ConfigDef.Type.STRING,
            DEFAULT_WHITELIST,
            ConfigDef.Importance.LOW,
            WHITELIST_DOC
        ).define(
            VOLUME_METRICS_REFRESH_PERIOD_MS,
            ConfigDef.Type.LONG,
            DEFAULT_VOLUME_METRICS_REFRESH_PERIOD,
            ConfigDef.Importance.LOW,
            VOLUME_METRICS_REFRESH_PERIOD_DOC
        );
  }

  public ConfluentMetricsReporterConfig(Properties props) {
    super(CONFIG, props);
  }

  public ConfluentMetricsReporterConfig(Map<String, ?> clientConfigs) {
    super(CONFIG, clientConfigs);
  }

  public static void main(String[] args) {
    System.out.println(CONFIG.toRst());
  }

  private static Map<String, Object> producerConfigDefaults() {
    Map<String, Object> defaults = new HashMap<>();
    defaults.putAll(MonitoringProducerDefaults.PRODUCER_CONFIG_DEFAULTS);
    defaults.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer"
    );
    defaults.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer"
    );
    defaults.put(ProducerConfig.CLIENT_ID_CONFIG, "confluent-metrics-reporter");
    return defaults;
  }

  static Properties getProducerProperties(Map<String, ?> clientConfigs) {
    Properties props = new Properties();
    props.putAll(producerConfigDefaults());
    props.putAll(getClientProperties(clientConfigs));
    return props;
  }

  static Properties getClientProperties(Map<String, ?> clientConfigs) {
    Properties props = new Properties();
    for (Map.Entry<String, ?> entry : clientConfigs.entrySet()) {
      if (entry.getKey().startsWith(METRICS_REPORTER_PREFIX)) {
        props.put(entry.getKey().substring(METRICS_REPORTER_PREFIX.length()), entry.getValue());
      }
    }

    // we require bootstrap servers
    Object bootstrap = props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    if (bootstrap == null) {
      throw new ConfigException(
          "Missing required property "
          + METRICS_REPORTER_PREFIX
          + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
      );
    }
    return props;
  }
}
