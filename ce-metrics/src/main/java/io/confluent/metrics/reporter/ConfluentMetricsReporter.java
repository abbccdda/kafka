// (Copyright) [2016 - 2016] Confluent, Inc.

package io.confluent.metrics.reporter;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricsRegistry;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import io.confluent.metrics.YammerMetricsUtils;
import io.confluent.metrics.record.ConfluentMetric;
import io.confluent.metrics.record.ConfluentMetric.KafkaMeasurable;
import io.confluent.metrics.record.ConfluentMetric.MetricsMessage;
import io.confluent.metrics.record.ConfluentMetric.MetricsMessage.Builder;
import io.confluent.metrics.record.ConfluentMetric.VolumeMetrics;
import io.confluent.metrics.record.ConfluentMetric.YammerGauge;
import io.confluent.metrics.record.ConfluentMetric.YammerHistogram;
import io.confluent.metrics.record.ConfluentMetric.YammerMeter;
import io.confluent.metrics.record.ConfluentMetric.YammerTimer;
import io.confluent.metrics.reporter.VolumeMetricsProvider.VolumeInfo;
import io.confluent.metrics.reporter.YammerMetricsHelper.YammerMetricsResult;
import io.confluent.serializers.ProtoSerde;
import kafka.server.KafkaConfig;

import static io.confluent.metrics.record.ConfluentMetric.MetricType.BROKER;
import static io.confluent.metrics.record.ConfluentMetric.MetricType.CONSUMER;
import static io.confluent.metrics.record.ConfluentMetric.MetricType.PRODUCER;
import static io.confluent.metrics.record.ConfluentMetric.MetricType.UNKNOWN;
import static io.confluent.metrics.reporter.ConfluentMetricsReporterConfig.VOLUME_METRICS_REFRESH_PERIOD_MS;

public class ConfluentMetricsReporter
    implements org.apache.kafka.common.metrics.MetricsReporter, ClusterResourceListener {

  private static final Logger log = LoggerFactory.getLogger(ConfluentMetricsReporter.class);

  //Yammer metric registry
  private static final MetricsRegistry METRICS_REGISTRY = Metrics.defaultRegistry();

  static {
    log.debug("available");
  }

  private final ProtoSerde<MetricsMessage> metricsMessageSerde =
      new ProtoSerde<>(MetricsMessage.getDefaultInstance());

  // Kafka metrics
  private final Map<MetricName, KafkaMetric> metricMap = new ConcurrentHashMap<>();
  private final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
  private final YammerMetricsHelper metricsHelper = new YammerMetricsHelper();

  private ConfluentMetricsReporterConfig metricsReporterConfig;
  private Properties clientProperties;
  private KafkaProducer<byte[], byte[]> producer;

  private long reportIntervalMs;
  private String publishTopic;
  private boolean createTopic;
  private Pattern pattern = null;
  private volatile String clusterId = null;
  private int brokerId;
  private String clientId;
  private String groupId;
  private ConfluentMetric.MetricType metricType;
  private long volumeMetricsRefreshPeriodMs =
      ConfluentMetricsReporterConfig.DEFAULT_VOLUME_METRICS_REFRESH_PERIOD;
  private String[] volumeMetricsLogDirs = new String[0];
  private com.sun.management.OperatingSystemMXBean osBean;


  @Override
  public void onUpdate(ClusterResource clusterResource) {
    this.clusterId = clusterResource.clusterId();

    log.info("Starting Confluent metrics reporter for cluster id {} with an interval of {} ms",
             clusterId, reportIntervalMs
    );
    executor.scheduleAtFixedRate(
        new MetricReportRunnable(),
        reportIntervalMs,
        reportIntervalMs,
        TimeUnit.MILLISECONDS
    );
  }

  /**
   * Configure this class with the given key-value pairs
   */
  public void configure(Map<String, ?> configs) {
    metricsReporterConfig = new ConfluentMetricsReporterConfig(configs);
    clientProperties = ConfluentMetricsReporterConfig.getClientProperties(configs);
    producer = new KafkaProducer<>(ConfluentMetricsReporterConfig.getProducerProperties(configs));

    reportIntervalMs =
        metricsReporterConfig.getLong(ConfluentMetricsReporterConfig.PUBLISH_PERIOD_CONFIG);
    publishTopic = metricsReporterConfig.getString(ConfluentMetricsReporterConfig.TOPIC_CONFIG);
    createTopic =
        metricsReporterConfig.getBoolean(ConfluentMetricsReporterConfig.TOPIC_CREATE_CONFIG);

    String regexString =
        metricsReporterConfig.getString(ConfluentMetricsReporterConfig.WHITELIST_CONFIG).trim();
    pattern = regexString.isEmpty() ? null : Pattern.compile(regexString);

    if (configs.containsKey(KafkaConfig.BrokerIdProp())) {
      metricType = BROKER;
    } else if (configs.containsKey(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)) {
      metricType = CONSUMER;
    } else if (configs.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
      metricType = PRODUCER;
    } else {
      metricType = UNKNOWN;
    }
    brokerId = metricType == BROKER
               ? Integer.parseInt((String) configs.get(KafkaConfig.BrokerIdProp()))
               : -1;

    boolean isClient = metricType == CONSUMER || metricType == PRODUCER;
    clientId =
        isClient && configs.containsKey(CommonClientConfigs.CLIENT_ID_CONFIG) ? (String) configs
            .get(CommonClientConfigs.CLIENT_ID_CONFIG) : "";
    groupId =
        (metricType == CONSUMER) && configs.containsKey(ConsumerConfig.GROUP_ID_CONFIG)
        ? (String) configs.get(ConsumerConfig.GROUP_ID_CONFIG)
        : "";

    if (configs.containsKey(VOLUME_METRICS_REFRESH_PERIOD_MS)) {
      volumeMetricsRefreshPeriodMs =
          Long.parseLong((String) configs.get(VOLUME_METRICS_REFRESH_PERIOD_MS));
    }
    String logDirsString = null;
    if (configs.containsKey("log.dirs")) {
      logDirsString = (String) configs.get("log.dirs");
    }
    if (logDirsString == null) {
      if (configs.containsKey("log.dir")) {
        logDirsString = (String) configs.get("log.dir");
      }
    }
    if (logDirsString != null) {
      volumeMetricsLogDirs = logDirsString.split("\\s*,\\s*");
    }
  }

  private boolean createTopicIfNotPresent() {
    int publishTopicReplicas =
        metricsReporterConfig.getInt(ConfluentMetricsReporterConfig.TOPIC_REPLICAS_CONFIG);
    int publishTopicPartitions =
        metricsReporterConfig.getInt(ConfluentMetricsReporterConfig.TOPIC_PARTITIONS_CONFIG);
    long retentionMs =
        metricsReporterConfig.getLong(ConfluentMetricsReporterConfig.TOPIC_RETENTION_MS_CONFIG);
    long retentionBytes =
        metricsReporterConfig.getLong(ConfluentMetricsReporterConfig.TOPIC_RETENTION_BYTES_CONFIG);
    long rollMs =
        metricsReporterConfig.getLong(ConfluentMetricsReporterConfig.TOPIC_ROLL_MS_CONFIG);
    int maxMessageBytes =
        metricsReporterConfig.getInt(ConfluentMetricsReporterConfig.TOPIC_MAX_MESSAGE_BYTES_CONFIG);

    try (final AdminClient adminClient = AdminClient.create(clientProperties)) {
      try {
        adminClient.describeTopics(Collections.singleton(publishTopic)).all().get();
        log.debug("Metrics reporter topic {} already exists", publishTopic);
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
          // something bad happened
          throw e;
        }
        // set minIsr to be consistent with
        // control center {@link io.confluent.controlcenter.util.TopicInfo.Builder.setReplication}
        int minIsr = Math.min(3, publishTopicReplicas < 3 ? 1 : publishTopicReplicas - 1);

        final Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "" + minIsr);
        topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "" + retentionMs);
        topicConfig.put(TopicConfig.RETENTION_BYTES_CONFIG, "" + retentionBytes);
        topicConfig.put(TopicConfig.SEGMENT_MS_CONFIG, "" + rollMs);
        topicConfig.put(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "" + maxMessageBytes);
        topicConfig.put(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.name);

        adminClient
            .createTopics(
                Collections.singleton(new NewTopic(
                    publishTopic,
                    publishTopicPartitions,
                    (short) publishTopicReplicas
                ).configs(topicConfig))
            )
            .all()
            .get();
        log.info("Created metrics reporter topic {}", publishTopic);
      }
      return true;
    } catch (ExecutionException e) {
      log.error("Error checking or creating metrics topic", e.getCause());
      return false;
    } catch (InterruptedException e) {
      log.warn("Confluent metrics reporter topic initialization interrupted");
      return false;
    }
  }

  /**
   * Called when the metrics repository is closed.
   */
  public void close() {
    log.info("Stopping Confluent metrics reporter");
    executor.shutdownNow();
    if (producer != null) {
      synchronized (producer) {
        producer.close(Duration.ofMillis(0));
      }
    }
  }

  /**
   * This is called when the reporter is first registered to initially register all existing
   * metrics
   *
   * @param metrics All currently existing metrics
   */
  public void init(List<KafkaMetric> metrics) {
    log.debug("initializing");
    for (KafkaMetric m : metrics) {
      metricMap.put(m.metricName(), m);
    }

    OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
    if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
      this.osBean = (com.sun.management.OperatingSystemMXBean) osBean;
    } else {
      log.warn("CPU metric is not available on this operating system");
    }

    executor.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    executor.setThreadFactory(new ThreadFactory() {
      public Thread newThread(Runnable runnable) {
        return ConfluentMetricsReporter.newThread("confluent-metrics-reporter-scheduler", runnable,
                true);
      }
    });
  }

  private static Thread newThread(String name, Runnable runnable, boolean daemon) {
    Thread thread = new Thread(runnable, name);
    thread.setDaemon(daemon);
    thread.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread t, Throwable e) {
        log.error("Uncaught exception in thread '{}':", t.getName(), e);
      }
    });
    return thread;
  }

  /**
   * This is called whenever a metric is updated or added
   */
  public void metricChange(KafkaMetric metric) {
    metricMap.put(metric.metricName(), metric);
  }

  /**
   * This is called whenever a metric is removed
   */
  public void metricRemoval(KafkaMetric metric) {
    log.debug("removing kafka metric : {}", metric.metricName());
    metricMap.remove(metric.metricName());
  }

  private class MetricReportRunnable implements Runnable {

    private boolean isTopicCreated = false;
    private VolumeMetricsProvider volumeMetricsProvider = null;

    public MetricReportRunnable() {
    }

    public void run() {
      if (volumeMetricsProvider == null) {
        volumeMetricsProvider =
            new VolumeMetricsProvider(volumeMetricsRefreshPeriodMs, volumeMetricsLogDirs);
      }
      try {
        if (createTopic) {
          if (!isTopicCreated) {
            isTopicCreated = createTopicIfNotPresent();
          }
          // if topic can't be created, skip the rest
          if (!isTopicCreated) {
            return;
          }
        }

        log.debug("Begin publishing metrics");

        Iterable<MetricsMessage> metricsMessages = genMetricsMessage();

        synchronized (producer) {
          // producer may already be closed if we are shutting down
          if (!Thread.currentThread().isInterrupted()) {
            for (MetricsMessage metricsMessage : metricsMessages) {
              log.trace("Generated metric message : {}", metricsMessage);
              producer.send(
                  new ProducerRecord<byte[], byte[]>(
                      publishTopic,
                      null,
                      metricsMessage.getTimestamp(),
                      null,
                      metricsMessageSerde.serialize(metricsMessage)
                  ),
                  new Callback() {
                    @Override
                    public void onCompletion(
                        RecordMetadata metadata,
                        Exception exception
                    ) {
                      if (exception != null) {
                        log.warn("Failed to produce metrics message", exception);
                      } else {
                        log.debug(
                            "Produced metrics message of size {} with "
                            + "offset {} to topic partition {}-{}",
                            metadata.serializedValueSize(),
                            metadata.offset(),
                            metadata.topic(),
                            metadata.partition()
                        );
                      }
                    }
                  }
              );
            }
          }
        }
      } catch (InterruptException e) {
        // broker is shutting shutdown, interrupt flag is taken care of by
        // InterruptException constructor
      } catch (Throwable t) {
        log.warn("Failed to publish metrics message in the reporter", t);
      }
    }

    private MetricsMessage.Builder getMetricsMessageBuilder(long timestamp) {
      MetricsMessage.Builder builder = MetricsMessage.newBuilder();
      builder.setMetricType(metricType);
      builder.setBrokerId(brokerId);
      builder.setClientId(clientId);
      builder.setGroupId(groupId);
      builder.setClusterId(clusterId == null ? "" : clusterId);
      builder.setTimestamp(timestamp);
      return builder;
    }

    private Iterable<MetricsMessage> genMetricsMessage() {
      long now = System.currentTimeMillis();
      Collection<MetricsMessage> out = new ArrayList<>();
      MetricsMessage.Builder systemMetricsMessageBuilder = getMetricsMessageBuilder(now);
      // Add system metrics
      ConfluentMetric.SystemMetrics.Builder systemMetricsBuilder =
          ConfluentMetric.SystemMetrics.newBuilder();
      Collection<VolumeInfo> volumeInfos = volumeMetricsProvider.getMetrics().values();
      for (VolumeInfo volumeInfo : volumeInfos) {
        VolumeMetrics.Builder bld = VolumeMetrics.newBuilder();
        bld.setName(volumeInfo.name());
        bld.setTotalBytes(volumeInfo.totalBytes());
        bld.setUsableBytes(volumeInfo.usableBytes());
        for (String logDir : volumeInfo.logDirs()) {
          bld.addLogDirs(ConfluentMetric.LogDir.newBuilder().setPath(logDir));
        }
        systemMetricsBuilder.addVolumes(bld);
      }
      systemMetricsMessageBuilder.setSystemMetrics(systemMetricsBuilder);
      out.add(systemMetricsMessageBuilder.build());

      // add Kafka metrics
      MetricsSplitter splitter = new MetricsSplitter(getMetricsMessageBuilder(now).buildPartial());
      splitter.addKafkaMeasurables(KafkaMetricsHelper.collectKafkaMetrics(metricMap, pattern));

      // add CPU metric if the source is a broker (we simulate a Kafka metric for serialization)
      if (metricType == BROKER) {
        if (ConfluentMetricsReporter.this.osBean != null) {
          double cpuUtil = osBean.getProcessCpuLoad();
          KafkaMeasurable.Builder builder = KafkaMeasurable.newBuilder();
          builder.setValue(cpuUtil);

          ConfluentMetric.KafkaMetricName.Builder nameBuilder = ConfluentMetric.KafkaMetricName.newBuilder();
          nameBuilder.setGroup("kafka.server");
          nameBuilder.setName("CpuUsage");
          builder.setMetricName(nameBuilder.build());
          KafkaMeasurable km = builder.build();
          List<KafkaMeasurable> kms = new ArrayList<>();
          kms.add(km);
          splitter.addKafkaMeasurables(kms);
        }
      }

      // add Yammer metrics if the source is a broker, the clients don't produce Yammer metrics
      if (metricType == BROKER) {
        YammerMetricsResult yammerMetricsResult =
            metricsHelper.collectYammerMetrics(METRICS_REGISTRY, pattern);
        splitter.addYammerGauges(yammerMetricsResult.gauges);
        splitter.addYammerMeters(yammerMetricsResult.meters);
        splitter.addYammerHistograms(yammerMetricsResult.histograms);
        splitter.addYammerTimers(yammerMetricsResult.timers);
      }

      out.addAll(splitter.build());
      return out;
    }

    class MetricsSplitter {

      MetricsMessage baseMetricsMessage;
      Map<String, MetricsMessage.Builder> messages = new HashMap<>();

      public MetricsSplitter(MetricsMessage baseMetricsMessage) {
        this.baseMetricsMessage = baseMetricsMessage;
      }

      Builder getBuilder(String metricName) {
        String topic = "";
        try {
          Map<String, String> tags = YammerMetricsUtils.extractTags(metricName);
          if (tags.containsKey("topic")) {
            topic = tags.get("topic");
          }
        } catch (IllegalArgumentException e) {
          // eat this.
        }
        if (!messages.containsKey(topic)) {
          messages.put(topic, MetricsMessage.newBuilder(baseMetricsMessage));
        }
        return messages.get(topic);
      }

      void addKafkaMeasurables(Collection<KafkaMeasurable> kafkaMeasurables) {
        for (KafkaMeasurable km : kafkaMeasurables) {
          String name = km.getMetricName().getName();
          Builder builder = getBuilder(name);
          builder.addKafkaMeasurable(km);
        }
      }

      void addYammerGauges(Collection<YammerGauge> yams) {
        for (YammerGauge yam : yams) {
          String name = yam.getMetricName().getMBeanName();
          Builder builder = getBuilder(name);
          builder.addYammerGauge(yam);
        }
      }

      void addYammerMeters(Collection<YammerMeter> yams) {
        for (YammerMeter yam : yams) {
          String name = yam.getMetricName().getMBeanName();
          Builder builder = getBuilder(name);
          builder.addYammerMeter(yam);
        }
      }

      void addYammerHistograms(Collection<YammerHistogram> yams) {
        for (YammerHistogram yam : yams) {
          String name = yam.getMetricName().getMBeanName();
          Builder builder = getBuilder(name);
          builder.addYammerHistogram(yam);
        }
      }

      void addYammerTimers(Collection<YammerTimer> yams) {
        for (YammerTimer yam : yams) {
          String name = yam.getMetricName().getMBeanName();
          Builder builder = getBuilder(name);
          builder.addYammerTimer(yam);
        }
      }

      Collection<MetricsMessage> build() {
        return FluentIterable.from(messages.values())
            .transform(
                new Function<Builder, MetricsMessage>() {
                  @Override
                  public MetricsMessage apply(Builder input) {
                    return input.build();
                  }
                }
            ).toList();
      }
    }
  }
}
