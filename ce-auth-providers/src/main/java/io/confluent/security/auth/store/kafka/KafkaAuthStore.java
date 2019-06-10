// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.kafka;

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.AuthKey;
import io.confluent.security.auth.store.data.AuthValue;
import io.confluent.security.auth.store.data.StatusKey;
import io.confluent.security.auth.store.data.StatusValue;
import io.confluent.security.auth.utils.MetricsUtils;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.store.MetadataStoreStatus;
import io.confluent.security.store.kafka.KafkaStoreConfig;
import io.confluent.security.store.kafka.clients.ConsumerListener;
import io.confluent.security.store.kafka.clients.JsonSerde;
import io.confluent.security.store.kafka.clients.KafkaReader;
import io.confluent.security.store.kafka.clients.StatusListener;
import io.confluent.security.store.kafka.coordinator.MetadataNodeManager;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAuthStore implements AuthStore, ConsumerListener<AuthKey, AuthValue> {

  private static final Logger log = LoggerFactory.getLogger(KafkaAuthStore.class);

  public static final String AUTH_TOPIC = KafkaStoreConfig.TOPIC_PREFIX + "-auth";
  private static final String METRIC_GROUP = "confluent.metadata";
  private static final String METRIC_TYPE = KafkaAuthStore.class.getSimpleName();

  private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);

  private final DefaultAuthCache authCache;
  private final Time time;
  private final int numAuthTopicPartitions;
  private final JsonSerde<AuthKey> keySerde;
  private final JsonSerde<AuthValue> valueSerde;
  private final Set<MetricName> metricNames;
  private final StoreStatusListener statusListener;
  private final List<Meter> successfulSendMeters;
  private final List<Meter> failedSendMeters;

  private KafkaStoreConfig clientConfig;
  private KafkaReader<AuthKey, AuthValue> reader;
  private volatile MetadataNodeManager nodeManager;
  private volatile KafkaAuthWriter writer;

  public KafkaAuthStore(Scope scope) {
    this(RbacRoles.loadDefaultPolicy(), Time.SYSTEM, scope, KafkaStoreConfig.NUM_PARTITIONS);
  }

  public KafkaAuthStore(RbacRoles rbacRoles, Time time, Scope scope, int numAuthTopicPartitions) {
    this.authCache = new DefaultAuthCache(rbacRoles, scope);
    this.time = time;
    this.numAuthTopicPartitions = numAuthTopicPartitions;

    this.statusListener = new StoreStatusListener();
    this.keySerde = JsonSerde.serde(AuthKey.class, true);
    this.valueSerde = JsonSerde.serde(AuthValue.class, false);

    successfulSendMeters = new ArrayList<>(numAuthTopicPartitions);
    failedSendMeters = new ArrayList<>(numAuthTopicPartitions);
    metricNames = new HashSet<>();

    metricNames.add(MetricsUtils.newGauge(METRIC_GROUP, METRIC_TYPE,
        "reader-failure-start-seconds-ago", Collections.emptyMap(),
        statusListener::secondsAfterReaderFailure));
    metricNames.add(MetricsUtils.newGauge(METRIC_GROUP, METRIC_TYPE,
        "remote-failure-start-seconds-ago", Collections.emptyMap(),
       statusListener::secondsAfterRemoteFailure));
    metricNames.add(MetricsUtils.newGauge(METRIC_GROUP, METRIC_TYPE,
        "active-writer-count", Collections.emptyMap(),
        () -> isMasterWriter() ? 1 : 0));

    IntStream.range(0, numAuthTopicPartitions).forEach(p -> {
      Map<String, String> tags =  Utils.mkMap(
          Utils.mkEntry("topic", AUTH_TOPIC),
          Utils.mkEntry("partition", String.valueOf(p)));
      metricNames.add(MetricsUtils.newGauge(METRIC_GROUP, METRIC_TYPE, "metadata-status", tags,
              () -> authCache.status(p).name()));
    });
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.clientConfig = new KafkaStoreConfig(configs);

    this.reader = new KafkaReader<>(AUTH_TOPIC,
        numAuthTopicPartitions,
        createConsumer(clientConfig.consumerConfigs(AUTH_TOPIC)),
        authCache,
        this,
        statusListener,
        time);

    log.debug("Configured auth store with configs {}", clientConfig);
  }

  @Override
  public DefaultAuthCache authCache() {
    if (this.reader == null)
      throw new IllegalStateException("Reader has not been started for this store");
    return authCache;
  }

  @Override
  public CompletionStage<Void> startReader() {
    return reader.start(clientConfig.topicCreateTimeout);
  }

  @Override
  public void startService(Collection<URL> nodeUrls) {
    if (nodeUrls == null || nodeUrls.isEmpty())
      throw new IllegalArgumentException("Server node URL not provided");

    if (nodeManager != null)
      throw new IllegalStateException("Writer has already been started for this store");
    log.debug("Starting writer for auth store {}", nodeUrls);

    metricNames.add(MetricsUtils.newGauge(METRIC_GROUP, METRIC_TYPE,
        "writer-failure-start-seconds-ago", Collections.emptyMap(),
        statusListener::secondsAfterWriterFailure));
    IntStream.range(0, numAuthTopicPartitions).forEach(p -> {
      Map<String, String> tags =  Utils.mkMap(
          Utils.mkEntry("topic", AUTH_TOPIC),
          Utils.mkEntry("partition", String.valueOf(p)));

      MetricName metricName = MetricsUtils.metricName(METRIC_GROUP, METRIC_TYPE, "record-send-rate", tags);
      successfulSendMeters.add(MetricsUtils.newMeter(metricName, "records"));
      metricNames.add(metricName);

      metricName = MetricsUtils.metricName(METRIC_GROUP, METRIC_TYPE, "record-error-rate", tags);
      failedSendMeters.add(MetricsUtils.newMeter(metricName, "records"));
      metricNames.add(metricName);
    });

    this.writer = createWriter(numAuthTopicPartitions, clientConfig, authCache, statusListener, time);
    nodeManager = createNodeManager(nodeUrls, clientConfig, writer, time);
    writer.rebalanceListener(nodeManager);
    nodeManager.start();
  }

  @Override
  public KafkaAuthWriter writer() {
    return writer;
  }

  @Override
  public boolean isMasterWriter() {
    if (nodeManager == null)
      throw new IllegalStateException("Writer has not been started for this store");
    else
      return nodeManager.isMasterWriter();
  }

  @Override
  public URL masterWriterUrl(String protocol) {
    if (nodeManager == null)
      throw new IllegalStateException("Writer has not been started for this store");
    else
      return nodeManager.masterWriterUrl(protocol);
  }

  @Override
  public Collection<URL> activeNodeUrls(String protocol) {
    if (nodeManager == null)
      throw new IllegalStateException("Writer has not been started for this store");
    else
      return nodeManager.activeNodeUrls(protocol);
  }

  @Override
  public void close() {
    log.debug("Closing auth store");
    long endMs = time.milliseconds() + CLOSE_TIMEOUT.toMillis();
    if (nodeManager != null)
      nodeManager.close(Duration.ofMillis(endMs - time.milliseconds()));
    if (writer != null)
      writer.close(Duration.ofMillis(Math.max(0, endMs - time.milliseconds())));
    if (reader != null)
      reader.close(Duration.ofMillis(Math.max(0, endMs - time.milliseconds())));
    MetricsUtils.removeMetrics(metricNames);
  }

  @Override
  public void onConsumerRecord(ConsumerRecord<AuthKey, AuthValue> record, AuthValue oldValue) {
    if (writer != null)
      writer.onConsumerRecord(record, oldValue);
    if (record.key() instanceof StatusKey) {
      int partition = record.partition();
      MetadataStoreStatus status = ((StatusValue) record.value()).status();
      switch (status) {
        case INITIALIZED:
          statusListener.onRemoteSuccess(partition);
          break;
        case FAILED:
          if (statusListener.onRemoteFailure(partition))
            throw new TimeoutException("Partition not successfully initialized within timeout " + partition);
          break;
        default:
          break;
      }
    }
  }

  // Visibility to override in tests
  protected Consumer<AuthKey, AuthValue> createConsumer(Map<String, Object> configs) {
    return new KafkaConsumer<>(configs, keySerde.deserializer(), valueSerde.deserializer());
  }

  // Visibility to override in tests
  protected Producer<AuthKey, AuthValue> createProducer(Map<String, Object> configs) {
    return new KafkaProducer<>(configs, keySerde.serializer(), valueSerde.serializer());
  }

  // Visibility to override in tests
  protected AdminClient createAdminClient(Map<String, Object> configs) {
    return KafkaAdminClient.create(configs);
  }

  // Visibility to override in tests
  protected MetadataNodeManager createNodeManager(Collection<URL> nodeUrls,
                                                  KafkaStoreConfig config,
                                                  KafkaAuthWriter writer,
                                                  Time time) {
    return new MetadataNodeManager(nodeUrls, config, writer, time);
  }

  // Visibility to override in tests
  protected KafkaAuthWriter createWriter(int numPartitions,
                                         KafkaStoreConfig clientConfig,
                                         DefaultAuthCache authCache,
                                         StatusListener statusListener,
                                         Time time) {
    return new KafkaAuthWriter(
        AUTH_TOPIC,
        numPartitions,
        clientConfig,
        createProducer(clientConfig.producerConfigs(AUTH_TOPIC)),
        () -> createAdminClient(clientConfig.adminClientConfigs()),
        authCache,
        statusListener,
        time);
  }

  // Visibility for unit test
  Long writerFailuresStartMs() {
    return statusListener.firstFailureMs(statusListener.writerFailuresStartMs);
  }

  // Visibility for unit test
  Long remoteFailuresStartMs() {
    return statusListener.firstFailureMs(statusListener.remoteFailuresStartMs);
  }

  private class StoreStatusListener implements StatusListener {

    private final AtomicLong readerFailureStartMs;
    private final ConcurrentHashMap<Integer, Long> writerFailuresStartMs;
    private final ConcurrentHashMap<Integer, Long> remoteFailuresStartMs;

    StoreStatusListener() {
      this.readerFailureStartMs = new AtomicLong(0);
      this.writerFailuresStartMs = new ConcurrentHashMap<>(numAuthTopicPartitions);
      this.remoteFailuresStartMs = new ConcurrentHashMap<>(numAuthTopicPartitions);
    }

    long secondsAfterReaderFailure() {
      return MetricsUtils.elapsedSeconds(time, readerFailureStartMs.get());
    }

    long secondsAfterWriterFailure() {
      return secondsAfterFailure(writerFailuresStartMs);
    }

    long secondsAfterRemoteFailure() {
      return secondsAfterFailure(remoteFailuresStartMs);
    }

    @Override
    public void onReaderSuccess() {
      readerFailureStartMs.set(0);
    }

    @Override
    public boolean onReaderFailure() {
      readerFailureStartMs.compareAndSet(0, time.milliseconds());
      return failed(readerFailureStartMs.get());
    }

    @Override
    public void onWriterSuccess(int partition) {
      writerFailuresStartMs.remove(partition);
    }

    @Override
    public boolean onWriterFailure(int partition) {
      writerFailuresStartMs.putIfAbsent(partition, time.milliseconds());
      return failed(firstFailureMs(writerFailuresStartMs));
    }

    @Override
    public void onProduceSuccess(int partition) {
      successfulSendMeters.get(partition).mark();
    }

    @Override
    public void onProduceFailure(int partition) {
      failedSendMeters.get(partition).mark();
    }

    @Override
    public void onRemoteSuccess(int partition) {
      remoteFailuresStartMs.remove(partition);
    }

    @Override
    public boolean onRemoteFailure(int partition) {
      remoteFailuresStartMs.putIfAbsent(partition, time.milliseconds());
      return failed(firstFailureMs(remoteFailuresStartMs));
    }

    private boolean failed(Long firstFailureMs) {
      if (firstFailureMs == null)
        return false;
      else
        return time.milliseconds() > firstFailureMs + clientConfig.retryTimeout.toMillis();
    }

    private long secondsAfterFailure(ConcurrentHashMap<Integer, Long> failuresStartMs) {
      Long firstFailureMs = firstFailureMs(failuresStartMs);
      if (firstFailureMs == null)
        return 0;
      else {
        return MetricsUtils.elapsedSeconds(time, firstFailureMs);
      }
    }

    private Long firstFailureMs(Map<Integer, Long> failuresStartMs) {
      Set<Long> failures = new HashSet<>(failuresStartMs.values());
      return failures.isEmpty() ? null : Collections.min(failures);
    }
  }
}
