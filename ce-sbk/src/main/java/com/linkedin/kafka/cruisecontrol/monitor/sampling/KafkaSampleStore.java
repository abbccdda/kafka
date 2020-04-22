/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.monitor.sampling;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.KafkaCruiseControlUtils;
import com.linkedin.kafka.cruisecontrol.metricsreporter.exception.UnknownVersionException;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.BrokerMetricSample;
import com.linkedin.kafka.cruisecontrol.monitor.sampling.holder.PartitionMetricSample;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.confluent.databalancer.StartupCheckInterruptedException;
import kafka.admin.RackAwareMode;
import kafka.controller.ReplicaAssignment;
import kafka.log.LogConfig;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.CorruptRecordException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.Option$;

import static com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils.ensureTopicNotUnderPartitionReassignment;

/**
 * The sample store that implements the {@link SampleStore}. It stores the partition metric samples and broker metric
 * samples back to Kafka and load from Kafka at startup.
 *
 * Required configurations for this class.
 * <ul>
 *   <li>{@link #PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG}: The config for the topic name of Kafka topic to store partition samples.</li>
 *   <li>{@link #BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG}: The config for the topic name of Kafka topic to store broker samples.</li>
 *   <li>{@link #NUM_SAMPLE_LOADING_THREADS_CONFIG}: The config for the number of Kafka sample store consumer threads, default value is
 *   set to {@link #DEFAULT_NUM_SAMPLE_LOADING_THREADS}.</li>
 *   <li>{@link #SAMPLE_STORE_TOPIC_REPLICATION_FACTOR_CONFIG}: The config for the replication factor of Kafka sample store topics,
 *   default value is set to {@link #DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR}.</li>
 *   <li>{@link #PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG}: The config for the number of partition for Kafka partition sample store
 *    topic, default value is set to {@link #DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT}.</li>
 *   <li>{@link #BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG}: The config for the number of partition for Kafka broker sample store topic,
 *   default value is set to {@link #DEFAULT_BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT}.</li>
 *   <li>{@link #MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG}: The config for the minimal retention time for Kafka partition sample
 *   store topic, default value is set to {@link #DEFAULT_MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS}.</li>
 *   <li>{@link #MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG}: The config for the minimal retention time for Kafka broker sample store
 *   topic, default value is set to {@link #DEFAULT_MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS}.</li>
 *   <li>{@link #SKIP_SAMPLE_STORE_TOPIC_RACK_AWARENESS_CHECK_CONFIG}: The config to skip checking sample store topics' replica distribution violate
 *   rack awareness property or not, default value is set to false.</li>
 * </ul>
 */
public class KafkaSampleStore implements SampleStore {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSampleStore.class);
  private static final String DEFAULT_CLEANUP_POLICY = "delete";
  // Keep additional windows in case some of the windows do not have enough samples.
  private static final int ADDITIONAL_WINDOW_TO_RETAIN_FACTOR = 2;
  private static final ConsumerRecords<byte[], byte[]> SHUTDOWN_RECORDS = new ConsumerRecords<>(Collections.emptyMap());
  private static final Duration SAMPLE_POLL_TIMEOUT = Duration.ofMillis(1000L);

  private static final String DEFAULT_PARTITION_SAMPLE_STORE_TOPIC = "_confluent_balancer_partition_samples";
  private static final String DEFAULT_BROKER_SAMPLE_STORE_TOPIC =  "_confluent_balancer_broker_samples";
  protected static final int DEFAULT_NUM_SAMPLE_LOADING_THREADS = 8;
  protected static final int DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR = 3;
  protected static final int DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT = 32;
  protected static final int DEFAULT_BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT = 32;
  protected static final long DEFAULT_MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS = 3600000L; // 1 hour
  protected static final long DEFAULT_MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS = 3600000L; // 1 hour
  protected static final boolean DEFAULT_SKIP_SAMPLE_STORE_TOPIC_RACK_AWARENESS_CHECK = true;
  protected static final long DEFAULT_RECONNECT_BACKOFF_MS = 50L;
  protected static final String PRODUCER_CLIENT_ID = "KafkaCruiseControlSampleStoreProducer";
  protected static final String CONSUMER_CLIENT_ID = "KafkaCruiseControlSampleStoreConsumer";
  protected static final Random RANDOM = new Random();
  protected List<KafkaConsumer<byte[], byte[]>> _consumers;
  protected ExecutorService _metricProcessorExecutor;
  protected String _partitionMetricSampleStoreTopic;
  protected String _brokerMetricSampleStoreTopic;
  protected volatile double _loadingProgress;
  protected Producer<byte[], byte[]> _producer;
  protected volatile boolean _shutdown = false;
  protected boolean _skipSampleStoreTopicRackAwarenessCheck;

  public static final String PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG = "partition.metric.sample.store.topic";
  public static final String BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG = "broker.metric.sample.store.topic";
  public static final String NUM_SAMPLE_LOADING_THREADS_CONFIG = "num.sample.loading.threads";
  public static final String SAMPLE_STORE_TOPIC_REPLICATION_FACTOR_CONFIG = "sample.store.topic.replication.factor";
  public static final String PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG = "partition.sample.store.topic.partition.count";
  public static final String BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG = "broker.sample.store.topic.partition.count";
  public static final String MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG = "min.partition.sample.store.topic.retention.time.ms";
  public static final String MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG = "min.broker.sample.store.topic.retention.time.ms";
  public static final String SKIP_SAMPLE_STORE_TOPIC_RACK_AWARENESS_CHECK_CONFIG = "skip.sample.store.topic.rack.awareness.check";

  @Override
  public void configure(Map<String, ?> config) {
    _partitionMetricSampleStoreTopic = getPartitionMetricSampleStoreTopic(config);
    _brokerMetricSampleStoreTopic = getBrokerMetricSampleStoreTopic(config);
    String numProcessingThreadsString = (String) config.get(NUM_SAMPLE_LOADING_THREADS_CONFIG);
    int numProcessingThreads = numProcessingThreadsString == null || numProcessingThreadsString.isEmpty()
                               ? DEFAULT_NUM_SAMPLE_LOADING_THREADS : Integer.parseInt(numProcessingThreadsString);
    String skipSampleStoreTopicRackAwarenessCheckString = (String) config.get(SKIP_SAMPLE_STORE_TOPIC_RACK_AWARENESS_CHECK_CONFIG);
    _skipSampleStoreTopicRackAwarenessCheck = skipSampleStoreTopicRackAwarenessCheckString == null
                                              || skipSampleStoreTopicRackAwarenessCheckString.isEmpty()
                                              ? DEFAULT_SKIP_SAMPLE_STORE_TOPIC_RACK_AWARENESS_CHECK
                                              : Boolean.parseBoolean(skipSampleStoreTopicRackAwarenessCheckString);
    _metricProcessorExecutor = Executors.newFixedThreadPool(numProcessingThreads);
    _consumers = new ArrayList<>(numProcessingThreads);
    for (int i = 0; i < numProcessingThreads; i++) {
      _consumers.add(createConsumer(config));
    }

    _producer = createProducer(config);
    _loadingProgress = -1.0;

    checkTopicPropertiesMaybeCreate(config, _consumers.get(0));
  }

  protected KafkaProducer<byte[], byte[]> createProducer(Map<String, ?> config) {
    Properties producerProps = new Properties();
    producerProps.putAll(KafkaCruiseControlUtils.filterProducerConfigs(config));
    String bootstrapServers = config.get(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG).toString();
    // Trim the brackets in List's String representation.
    if (bootstrapServers.length() > 2) {
      bootstrapServers = bootstrapServers.substring(1, bootstrapServers.length() - 1);
    }
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    producerProps.setProperty(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_ID);
    // Set batch.size and linger.ms to a big number to have better batching.
    producerProps.setProperty(ProducerConfig.LINGER_MS_CONFIG, "30000");
    producerProps.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "500000");
    producerProps.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");
    producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, "5");
    producerProps.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.setProperty(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG,
                              config.get(KafkaCruiseControlConfig.RECONNECT_BACKOFF_MS_CONFIG).toString());
    return new KafkaProducer<>(producerProps);
  }

  protected static KafkaConsumer<byte[], byte[]> createConsumer(Map<String, ?> config) {
    Properties consumerProps = new Properties();
    consumerProps.putAll(KafkaCruiseControlUtils.filterConsumerConfigs(config));
    long randomToken = RANDOM.nextLong();
    String bootstrapServers = config.get(KafkaCruiseControlConfig.BOOTSTRAP_SERVERS_CONFIG).toString();
    // Trim the brackets in List's String representation.
    if (bootstrapServers.length() > 2) {
      bootstrapServers = bootstrapServers.substring(1, bootstrapServers.length() - 1);
    }
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "KafkaCruiseControlSampleStore" + randomToken);
    consumerProps.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_CLIENT_ID + randomToken);
    consumerProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(Integer.MAX_VALUE));
    consumerProps.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.setProperty(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG,
                                             config.get(KafkaCruiseControlConfig.RECONNECT_BACKOFF_MS_CONFIG).toString());
    return new KafkaConsumer<>(consumerProps);
  }

  /**
   * Make sure any condition needed to start this {@code CruiseControlComponent} is satisfied.
   */
  public static void checkStartupCondition(KafkaCruiseControlConfig config,
                                           Semaphore abortStartupCheck) throws InterruptedException {
    Map<String, Object> configPairs = config.mergedConfigValues();
    try (KafkaConsumer<byte[], byte[]> consumer = createConsumer(configPairs)) {
      long maxTimeoutSec = 60;
      long currentTimeoutInSec = 1;
      while (!checkTopicsCreated(configPairs, consumer)) {
        LOG.info("Waiting for {} seconds to ensure that sample store topics are created/exists.",
                currentTimeoutInSec);
        if (abortStartupCheck.tryAcquire(currentTimeoutInSec, TimeUnit.SECONDS)) {
          throw new StartupCheckInterruptedException();
        }
        currentTimeoutInSec = Math.min(2 * currentTimeoutInSec, maxTimeoutSec);
      }
    }
  }

  private static String getBrokerMetricSampleStoreTopic(Map<String, ?> config) {
    String brokerSampleStoreTopic = (String) config.get(BROKER_METRIC_SAMPLE_STORE_TOPIC_CONFIG);
    return brokerSampleStoreTopic == null || brokerSampleStoreTopic.isEmpty()
            ? DEFAULT_BROKER_SAMPLE_STORE_TOPIC
            : brokerSampleStoreTopic;
  }

  private static String getPartitionMetricSampleStoreTopic(Map<String, ?> config) {
    String partitionSampleStoreTopic = (String) config.get(PARTITION_METRIC_SAMPLE_STORE_TOPIC_CONFIG);
    return partitionSampleStoreTopic == null || partitionSampleStoreTopic.isEmpty()
            ? DEFAULT_PARTITION_SAMPLE_STORE_TOPIC
            : partitionSampleStoreTopic;
  }


  private static int getBrokerSampleStoreTopicPartitionCount(Map<String, ?> config) {
    String brokerSampleStoreTopicPartitionCount = (String) config.get(BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG);
    return brokerSampleStoreTopicPartitionCount == null || brokerSampleStoreTopicPartitionCount.isEmpty()
            ? DEFAULT_BROKER_SAMPLE_STORE_TOPIC_PARTITION_COUNT
            : Integer.parseInt(brokerSampleStoreTopicPartitionCount);
  }

  private static int getPartitionSampleStoreTopicPartitionCount(Map<String, ?> config) {
    String partitionSampleStoreTopicPartitionCount = (String) config.get(PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT_CONFIG);
    return partitionSampleStoreTopicPartitionCount == null || partitionSampleStoreTopicPartitionCount.isEmpty()
            ? DEFAULT_PARTITION_SAMPLE_STORE_TOPIC_PARTITION_COUNT
            : Integer.parseInt(partitionSampleStoreTopicPartitionCount);
  }

  private static int getSampleStoreTopicReplicationFactor(Map<String, ?> config) {
    String replicationFactorConfig = (String) config.get(SAMPLE_STORE_TOPIC_REPLICATION_FACTOR_CONFIG);
    return replicationFactorConfig == null || replicationFactorConfig.isEmpty()
            ? DEFAULT_SAMPLE_STORE_TOPIC_REPLICATION_FACTOR
            : Integer.parseInt(replicationFactorConfig);
  }

  private static long getMinPartitionRetentionTimeMs(Map<String, ?> config) {
    String minPartitionRetentionTimeMs = (String) config.get(MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG);
    return minPartitionRetentionTimeMs == null || minPartitionRetentionTimeMs.isEmpty()
            ? DEFAULT_MIN_PARTITION_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS
            : Long.parseLong(minPartitionRetentionTimeMs);
  }

  private static long getMinBrokerRetentionTimeMs(Map<String, ?> config) {
    String minBrokerRetentionTimeMs = (String) config.get(MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS_CONFIG);
    return minBrokerRetentionTimeMs == null || minBrokerRetentionTimeMs.isEmpty()
            ? DEFAULT_MIN_BROKER_SAMPLE_STORE_TOPIC_RETENTION_TIME_MS
            : Long.parseLong(minBrokerRetentionTimeMs);
  }

  /**
   * A wrapper around {@code #ensureTopicsCreated} method that catches all errors and returns
   * a boolean indicating if the sample store topics exist (or can be successfully created).
   * Visible for testing
   */
  static boolean checkTopicsCreated(Map<String, ?> config, KafkaConsumer<byte[], byte[]> consumer) {
    try {
      return checkTopicPropertiesMaybeCreate(config, consumer);
    } catch (Exception ex) {
      LOG.error("Error when checking for sample store topics: {}", ex.getMessage());
      LOG.debug("Error: ", ex);
      return false;
    }
  }

  @SuppressWarnings("unchecked")
  private static boolean checkTopicPropertiesMaybeCreate(Map<String, ?> config, KafkaConsumer<byte[], byte[]> consumer) {
    String connectString = (String) config.get(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG);
    boolean zkSecurityEnabled = (Boolean) config.get(KafkaCruiseControlConfig.ZOOKEEPER_SECURITY_ENABLED_CONFIG);
    KafkaZkClient kafkaZkClient = KafkaCruiseControlUtils.createKafkaZkClient(connectString,
        "KafkaSampleStore",
        "EnsureTopicCreated",
        zkSecurityEnabled);
    AdminZkClient adminZkClient = new AdminZkClient(kafkaZkClient);
    AdminClient adminClient = KafkaCruiseControlUtils.createAdminClient(KafkaCruiseControlUtils.filterAdminClientConfigs(config));
    try {
      long partitionSampleWindowMs = (Long) config.get(KafkaCruiseControlConfig.PARTITION_METRICS_WINDOW_MS_CONFIG);
      long brokerSampleWindowMs = (Long) config.get(KafkaCruiseControlConfig.BROKER_METRICS_WINDOW_MS_CONFIG);

      int numPartitionSampleWindows = (Integer) config.get(KafkaCruiseControlConfig.NUM_PARTITION_METRICS_WINDOWS_CONFIG);
      long partitionSampleRetentionMs = (numPartitionSampleWindows * ADDITIONAL_WINDOW_TO_RETAIN_FACTOR) * partitionSampleWindowMs;
      partitionSampleRetentionMs = Math.max(getMinPartitionRetentionTimeMs(config), partitionSampleRetentionMs);

      int numBrokerSampleWindows = (Integer) config.get(KafkaCruiseControlConfig.NUM_BROKER_METRICS_WINDOWS_CONFIG);
      long brokerSampleRetentionMs = (numBrokerSampleWindows * ADDITIONAL_WINDOW_TO_RETAIN_FACTOR) * brokerSampleWindowMs;
      brokerSampleRetentionMs = Math.max(getMinBrokerRetentionTimeMs(config), brokerSampleRetentionMs);

      int numberOfBrokersInCluster = kafkaZkClient.getAllBrokersInCluster().size();

      int replicationFactor = getSampleStoreTopicReplicationFactor(config);
      if (numberOfBrokersInCluster < replicationFactor) {
        throw new IllegalStateException(
            String.format("Kafka cluster has %d brokers but the requested replication factor is %d " +
                    "(brokers in cluster=%d, zookeeper.connect=%s)", numberOfBrokersInCluster,
                replicationFactor, numberOfBrokersInCluster,
                config.get(KafkaCruiseControlConfig.ZOOKEEPER_CONNECT_CONFIG)));
      }

      Map<String, List<PartitionInfo>> topics = consumer.listTopics();
      return ensureTopicCreated(kafkaZkClient, adminZkClient, adminClient, topics.keySet(),
                    getPartitionMetricSampleStoreTopic(config), partitionSampleRetentionMs,
                    replicationFactor, getPartitionSampleStoreTopicPartitionCount(config))
              &
              ensureTopicCreated(kafkaZkClient, adminZkClient, adminClient, topics.keySet(),
                      getBrokerMetricSampleStoreTopic(config), brokerSampleRetentionMs,
                      replicationFactor, getBrokerSampleStoreTopicPartitionCount(config));
    } finally {
      KafkaCruiseControlUtils.closeKafkaZkClientWithTimeout(kafkaZkClient);
      KafkaCruiseControlUtils.closeAdminClientWithTimeout(adminClient);
    }
  }

  /**
   * Add new partitions to the Kafka topic.
   *
   * @param kafkaZkClient KafkaZkClient class to use to add new partitions.
   * @param adminZkClient AdminZkClient class to use to add new partitions.
   * @param topic The topic to apply the change.
   * @param topicDescription The description of the topic ot apply the change.
   * @param partitionCount The target partition count of the topic.
   */
  @SuppressWarnings("unchecked")
  private static void maybeIncreaseTopicPartitionCount(KafkaZkClient kafkaZkClient,
                                                       AdminZkClient adminZkClient,
                                                       String topic,
                                                       TopicDescription topicDescription,
                                                       int partitionCount) {
    if (partitionCount > topicDescription.partitions().size()) {
      if (!ensureTopicNotUnderPartitionReassignment(kafkaZkClient, topic)) {
        LOG.warn("There are ongoing partition reassignments for topic {}, skip checking its partition count.", topic);
        return;
      }
      scala.collection.mutable.Map<Object, ReplicaAssignment> existingAssignment = new scala.collection.mutable.HashMap<>();
      scala.collection.immutable.Set<String> topics = (scala.collection.immutable.Set<String>)
          scala.collection.immutable.Set$.MODULE$.apply(
              JavaConverters.asScalaBufferConverter(Collections.singletonList(topic)).asScala().toSeq());
      JavaConverters.asJavaIterable(kafkaZkClient.getFullReplicaAssignmentForTopics(topics))
          .forEach(e -> existingAssignment.put(e._1.partition(), e._2));
      LOG.info("DataBalancer: Adjusting sample store topic {} partition count to {}", topic, partitionCount);
      adminZkClient.addPartitions(topic, existingAssignment, adminZkClient.getBrokerMetadatas(RackAwareMode.Safe$.MODULE$, null),
                                  partitionCount, null, false, null, Option$.MODULE$.empty());
      LOG.info("Kafka topic " + topic + " now has " + partitionCount + " partitions.");
    }
  }

  /**
   * Check that topic "topic" exists and has the correct configuration. If the topic does not exist, it
   * will be created. Return value indicates if the topic did exist (but NOT that the topic configuration
   * has been updated to match the desired configuration).
   *
   * @param kafkaZkClient -- KafkaZK Client to use for topic create/update operations
   * @param adminZkClient -- AdminZK client to use for topic create/update operations
   * @param adminClient -- for topic describe operations
   * @param allTopics -- List of topics known to the system
   * @param topic -- topic whose existence and configuration is under question
   * @param retentionMs -- desired retention period for the topic
   * @param replicationFactor -- desired replication factor for the topic
   * @param partitionCount -- desired partition count for the topic
   * @return true if the topic exists and false if the topic had to be created.
   */
  private static boolean ensureTopicCreated(KafkaZkClient kafkaZkClient,
                                         AdminZkClient adminZkClient,
                                         AdminClient adminClient,
                                         Set<String> allTopics,
                                         String topic,
                                         long retentionMs,
                                         int replicationFactor,
                                         int partitionCount) {
    Properties props = new Properties();
    props.setProperty(LogConfig.RetentionMsProp(), Long.toString(retentionMs));
    props.setProperty(LogConfig.CleanupPolicyProp(), DEFAULT_CLEANUP_POLICY);
    if (!allTopics.contains(topic)) {
      LOG.info("DataBalancer: Creating sample store topic {} ", topic);
      adminZkClient.createTopic(topic, partitionCount, replicationFactor, props, RackAwareMode.Safe$.MODULE$, false, Option$.MODULE$.empty());
      return false;
    } else {
      try {
        LOG.info("DataBalancer: Adjusting sample store topic {} configuration", topic);
        adminZkClient.changeTopicConfig(topic, props);
        TopicDescription topicDescription;
        try {
          topicDescription = adminClient.describeTopics(Collections.singleton(topic)).values().get(topic).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e.getMessage());
        }
        maybeIncreaseTopicPartitionCount(kafkaZkClient, adminZkClient, topic, topicDescription, partitionCount);
      }  catch (RuntimeException re) {
        LOG.error("Skip updating configuration of topic " +  topic + " due to exception.", re);
      }
      return true;
    }
  }

  @Override
  public void storeSamples(MetricSampler.Samples samples) {
    final AtomicInteger metricSampleCount = new AtomicInteger(0);
    for (PartitionMetricSample sample : samples.partitionMetricSamples()) {
      _producer.send(new ProducerRecord<>(_partitionMetricSampleStoreTopic, null, sample.sampleTime(), null, sample.toBytes()),
                     new Callback() {
                       @Override
                       public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                         if (e == null) {
                           metricSampleCount.incrementAndGet();
                         } else {
                           LOG.error("Failed to produce partition metric sample for {} of timestamp {} due to exception",
                                     sample.entity().tp(), sample.sampleTime(), e);
                         }
                       }
                     });
    }
    final AtomicInteger brokerMetricSampleCount = new AtomicInteger(0);
    for (BrokerMetricSample sample : samples.brokerMetricSamples()) {
      _producer.send(new ProducerRecord<>(_brokerMetricSampleStoreTopic, sample.toBytes()),
                     new Callback() {
                       @Override
                       public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                         if (e == null) {
                           brokerMetricSampleCount.incrementAndGet();
                         } else {
                           LOG.error("Failed to produce model training sample due to exception", e);
                         }
                       }
                     });
    }
    _producer.flush();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stored {} partition metric samples and {} broker metric samples to Kafka",
                metricSampleCount.get(), brokerMetricSampleCount.get());
    }
  }

  @Override
  public void loadSamples(SampleLoader sampleLoader) {
    LOG.info("Starting loading samples.");
    long startMs = System.currentTimeMillis();
    AtomicLong numPartitionMetricSamples = new AtomicLong(0L);
    AtomicLong numBrokerMetricSamples = new AtomicLong(0L);
    AtomicLong totalSamples = new AtomicLong(0L);
    AtomicLong numLoadedSamples = new AtomicLong(0L);
    try {
      prepareConsumers();

      for (KafkaConsumer<byte[], byte[]> consumer : _consumers) {
        _metricProcessorExecutor.submit(
            new MetricLoader(consumer, sampleLoader, numLoadedSamples, numPartitionMetricSamples, numBrokerMetricSamples,
                             totalSamples));
      }
      // Blocking waiting for the metric loading to finish.
      _metricProcessorExecutor.shutdown();
      _metricProcessorExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      LOG.error("Received exception when loading samples", e);
    } finally {
      _consumers.forEach(Consumer::close);
      try {
        _metricProcessorExecutor.awaitTermination(30000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted during waiting for metrics processor to shutdown.");
      }
    }
    long endMs = System.currentTimeMillis();
    long addedPartitionSampleCount = sampleLoader.partitionSampleCount();
    long addedBrokerSampleCount = sampleLoader.brokerSampleCount();
    long discardedPartitionMetricSamples = numPartitionMetricSamples.get() - addedPartitionSampleCount;
    long discardedBrokerMetricSamples = numBrokerMetricSamples.get() - addedBrokerSampleCount;
    LOG.info("Sample loading finished. Loaded {}{} partition metrics samples and {}{} broker metric samples in {} ms.",
             addedPartitionSampleCount,
             discardedPartitionMetricSamples > 0 ? String.format("(%d discarded)", discardedPartitionMetricSamples) : "",
             sampleLoader.brokerSampleCount(),
             discardedBrokerMetricSamples > 0 ? String.format("(%d discarded)", discardedBrokerMetricSamples) : "",
             endMs - startMs);
  }

  @Override
  public double sampleLoadingProgress() {
    return _loadingProgress;
  }

  @Override
  public void evictSamplesBefore(long timestamp) {
    //TODO: use the deleteMessageBefore method to delete old samples.
  }

  @Override
  public void close() {
    _shutdown = true;
    _producer.close(300L, TimeUnit.SECONDS);
  }

  private void prepareConsumers() {
    int numConsumers = _consumers.size();
    List<List<TopicPartition>> assignments = new ArrayList<>();
    for (int i = 0; i < numConsumers; i++) {
      assignments.add(new ArrayList<>());
    }
    int j = 0;
    for (String topic : Arrays.asList(_partitionMetricSampleStoreTopic, _brokerMetricSampleStoreTopic)) {
      for (PartitionInfo partInfo : _consumers.get(0).partitionsFor(topic)) {
        assignments.get(j++ % numConsumers).add(new TopicPartition(partInfo.topic(), partInfo.partition()));
      }
    }
    for (int i = 0; i < numConsumers; i++) {
      _consumers.get(i).assign(assignments.get(i));
    }
  }

  private class MetricLoader implements Runnable {
    private final SampleLoader _sampleLoader;
    private final AtomicLong _numLoadedSamples;
    private final AtomicLong _numPartitionMetricSamples;
    private final AtomicLong _numBrokerMetricSamples;
    private final AtomicLong _totalSamples;
    private final KafkaConsumer<byte[], byte[]> _consumer;

    MetricLoader(KafkaConsumer<byte[], byte[]> consumer,
                 SampleLoader sampleLoader,
                 AtomicLong numLoadedSamples,
                 AtomicLong numPartitionMetricSamples,
                 AtomicLong numBrokerMetricSamples,
                 AtomicLong totalSamples) {
      _consumer = consumer;
      _sampleLoader = sampleLoader;
      _numLoadedSamples = numLoadedSamples;
      _numPartitionMetricSamples = numPartitionMetricSamples;
      _numBrokerMetricSamples = numBrokerMetricSamples;
      _totalSamples = totalSamples;
    }

    @Override
    public void run() {
      try {
        prepareConsumerOffset();
        Map<TopicPartition, Long> beginningOffsets = _consumer.beginningOffsets(_consumer.assignment());
        Map<TopicPartition, Long> endOffsets = _consumer.endOffsets(_consumer.assignment());
        LOG.debug("Loading beginning offsets: {}, loading end offsets: {}", beginningOffsets, endOffsets);
        for (Map.Entry<TopicPartition, Long> entry : beginningOffsets.entrySet()) {
          _totalSamples.addAndGet(endOffsets.get(entry.getKey()) - entry.getValue());
          _loadingProgress = (double) _numLoadedSamples.get() / _totalSamples.get();
        }
        while (!sampleLoadingFinished(endOffsets)) {
          try {
            ConsumerRecords<byte[], byte[]> consumerRecords = _consumer.poll(SAMPLE_POLL_TIMEOUT);
            if (consumerRecords == SHUTDOWN_RECORDS) {
              LOG.trace("Metric loader received empty records");
              return;
            }
            Set<PartitionMetricSample> partitionMetricSamples = new HashSet<>();
            Set<BrokerMetricSample> brokerMetricSamples = new HashSet<>();
            for (ConsumerRecord<byte[], byte[]> record : consumerRecords) {
              try {
                if (record.topic().equals(_partitionMetricSampleStoreTopic)) {
                  PartitionMetricSample sample = PartitionMetricSample.fromBytes(record.value());
                  partitionMetricSamples.add(sample);
                  LOG.trace("Loaded partition metric sample {}", sample);
                } else if (record.topic().equals(_brokerMetricSampleStoreTopic)) {
                  BrokerMetricSample sample = BrokerMetricSample.fromBytes(record.value());
                  // For some legacy BrokerMetricSample, there is no timestamp in the broker samples. In this case
                  // we use the record timestamp as the broker metric timestamp.
                  sample.close(record.timestamp());
                  brokerMetricSamples.add(sample);
                  LOG.trace("Loaded broker metric sample {}", sample);
                }
              } catch (UnknownVersionException e) {
                LOG.warn("Ignoring sample due to", e);
              }
            }
            if (!partitionMetricSamples.isEmpty() || !brokerMetricSamples.isEmpty()) {
              _sampleLoader.loadSamples(new MetricSampler.Samples(partitionMetricSamples, brokerMetricSamples));
              _numPartitionMetricSamples.getAndAdd(partitionMetricSamples.size());
              _numBrokerMetricSamples.getAndAdd(brokerMetricSamples.size());
              _loadingProgress = (double) _numLoadedSamples.addAndGet(consumerRecords.count()) / _totalSamples.get();
            }
          } catch (CorruptRecordException cre) {
            LOG.warn("Corrupt message detected, skipping forward");
            for (TopicPartition tp : _consumer.assignment()) {
              long position = _consumer.position(tp);
              if (position < endOffsets.get(tp)) {
                _consumer.seek(tp, position + 1);
              }
            }
          } catch (Exception e) {
            if (_shutdown) {
              return;
            } else {
              LOG.error("Metric loader received exception:", e);
            }
          }
        }
        LOG.info("Metric loader finished loading samples.");
      } catch (Throwable t) {
        LOG.warn("Encountered error when loading sample from Kafka.", t);
      }
    }

    private boolean sampleLoadingFinished(Map<TopicPartition, Long> endOffsets) {
      for (Map.Entry<TopicPartition, Long> entry : endOffsets.entrySet()) {
        long position = _consumer.position(entry.getKey());
        if (position < entry.getValue()) {
          LOG.debug("Partition {} is still lagging. Current position: {}, LEO: {}", entry.getKey(),
                    position, entry.getValue());
          return false;
        }
      }
      return true;
    }

    /**
     * Config the sample loading consumers to consume from proper starting offsets. The sample store Kafka topic may contain data
     * which are too old for {@link com.linkedin.cruisecontrol.monitor.sampling.aggregator.MetricSampleAggregator} to keep in memory,
     * to prevent loading these stale data, manually seek the consumers' staring offset to the offset at proper timestamp.
     */
    private void prepareConsumerOffset() {
      Map<TopicPartition, Long> beginningTimestamp = new HashMap<>(_consumer.assignment().size());
      long currentTimeMs = System.currentTimeMillis();
      for (TopicPartition tp : _consumer.assignment()) {
        if (tp.topic().equals(_brokerMetricSampleStoreTopic)) {
          beginningTimestamp.put(tp, currentTimeMs - _sampleLoader.brokerMonitoringPeriodMs());
        } else {
          beginningTimestamp.put(tp, currentTimeMs - _sampleLoader.partitionMonitoringPeriodMs());
        }
      }

      Set<TopicPartition> partitionWithNoRecentMessage = new HashSet<>();
      Map<TopicPartition, OffsetAndTimestamp> beginningOffsetAndTimestamp = _consumer.offsetsForTimes(beginningTimestamp);
      for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry: beginningOffsetAndTimestamp.entrySet()) {
        if (entry.getValue() == null) {
          // If this sample store topic partition does not have data available after beginning timestamp, then seek to the
          // beginning of this topic partition.
          partitionWithNoRecentMessage.add(entry.getKey());
        } else {
          _consumer.seek(entry.getKey(), entry.getValue().offset());
        }
      }
      if (partitionWithNoRecentMessage.size() > 0) {
        _consumer.seekToBeginning(partitionWithNoRecentMessage);
      }
    }
  }
}
