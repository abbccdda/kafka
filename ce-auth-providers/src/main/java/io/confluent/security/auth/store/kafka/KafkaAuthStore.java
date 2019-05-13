// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.kafka;

import io.confluent.security.auth.metadata.AuthStore;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.AuthKey;
import io.confluent.security.auth.store.data.AuthValue;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.store.kafka.KafkaStoreConfig;
import io.confluent.security.store.kafka.clients.ConsumerListener;
import io.confluent.security.store.kafka.clients.JsonSerde;
import io.confluent.security.store.kafka.clients.KafkaReader;
import io.confluent.security.store.kafka.coordinator.MetadataNodeManager;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaAuthStore implements AuthStore, ConsumerListener<AuthKey, AuthValue> {

  private static final Logger log = LoggerFactory.getLogger(KafkaAuthStore.class);

  public static final String AUTH_TOPIC = "__confluent_security_auth";

  private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);

  private final DefaultAuthCache authCache;
  private final Time time;
  private final JsonSerde<AuthKey> keySerde;
  private final JsonSerde<AuthValue> valueSerde;

  private KafkaStoreConfig clientConfig;
  private KafkaReader<AuthKey, AuthValue> reader;
  private volatile MetadataNodeManager nodeManager;
  private volatile KafkaAuthWriter writer;

  public KafkaAuthStore(Scope scope) {
    this(RbacRoles.loadDefaultPolicy(), Time.SYSTEM, scope);
  }

  public KafkaAuthStore(RbacRoles rbacRoles, Time time, Scope scope) {
    this.authCache = new DefaultAuthCache(rbacRoles, scope);
    this.time = time;

    this.keySerde = JsonSerde.serde(AuthKey.class, true);
    this.valueSerde = JsonSerde.serde(AuthValue.class, false);
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.clientConfig = new KafkaStoreConfig(configs);

    this.reader = new KafkaReader<>(AUTH_TOPIC,
        createConsumer(clientConfig.readerConfigs()),
        authCache,
        this,
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
    return reader.start(() -> createAdminClient(clientConfig.adminClientConfigs(true)),
        clientConfig.topicCreateTimeout);
  }

  @Override
  public void startService(Collection<URL> nodeUrls) {
    if (nodeUrls == null ||  nodeUrls.isEmpty())
      throw new IllegalArgumentException("Server node URL not provided");

    if (nodeManager != null)
      throw new IllegalStateException("Writer has already been started for this store");
    log.debug("Starting writer for auth store {}", nodeUrls);

    this.writer = new KafkaAuthWriter(
        AUTH_TOPIC,
        clientConfig,
        createProducer(clientConfig.writerConfigs()),
        () -> createAdminClient(clientConfig.adminClientConfigs(false)),
        authCache,
        time);

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
  }

  @Override
  public void onConsumerRecord(ConsumerRecord<AuthKey, AuthValue> record, AuthValue oldValue) {
    if (writer != null)
      writer.onConsumerRecord(record, oldValue);
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
}
