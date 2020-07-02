/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.security.audit.telemetry.exporter;

import static io.confluent.security.audit.telemetry.exporter.NonBlockingKafkaExporterConfig.BOOTSTRAP_SERVERS_CONFIG;
import static io.confluent.security.audit.telemetry.exporter.NonBlockingKafkaExporterConfig.EVENT_LOGGER_LOG_BLOCKING_CONFIG;
import static io.confluent.telemetry.events.EventLoggerConfig.CLOUD_EVENT_BINARY_ENCODING;
import static io.confluent.telemetry.events.EventLoggerConfig.CLOUD_EVENT_STRUCTURED_ENCODING;

import com.google.protobuf.MessageLite;
import io.cloudevents.CloudEvent;
import io.cloudevents.v1.AttributesImpl;
import io.confluent.telemetry.events.EventLoggerConfig;
import io.confluent.telemetry.events.cloudevents.extensions.RouteExtension;
import io.confluent.telemetry.events.serde.Protobuf;
import io.confluent.telemetry.events.serde.Serializer;
import io.confluent.telemetry.events.exporter.Exporter;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The NonBlockingKafkaExporter sends events to topics based on their content. It doesnot block on
 * producer metadata refresh or creation of topics.
 */
public class NonBlockingKafkaExporter<T extends MessageLite>
    implements Exporter<CloudEvent<AttributesImpl, T>> {

  private static final Logger log = LoggerFactory.getLogger(NonBlockingKafkaExporter.class);
  // TODO: Make this a property.
  private static final long TOPIC_READY_TIMEOUT_MS = 15_000L;
  private static final long TOPIC_PARTITION_TIMEOUT_MS = 1_000L;

  private KafkaProducer<String, byte[]> producer;
  private volatile boolean isClosing = false;

  private boolean createTopic;
  private Properties producerProperties;
  private String defaultRoute;
  private TopicManager topicManager;
  private Instant metadataRefreshed;
  private ScheduledThreadPoolExecutor metadataRefresh;

  private NonBlockingKafkaExporterConfig config;
  private Serializer<T> protobufSerializer;

  public NonBlockingKafkaExporter() {
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.config = new NonBlockingKafkaExporterConfig(configs);

    this.createTopic = config.getBoolean(NonBlockingKafkaExporterConfig.TOPIC_CREATE_CONFIG);
    this.defaultRoute = NonBlockingKafkaExporterConfig.DEFAULT_TOPIC;

    this.producerProperties = config.producerProperties();
    this.producer = new KafkaProducer<>(this.producerProperties);

    String encoding = new EventLoggerConfig(configs).getString(EventLoggerConfig.CLOUD_EVENT_ENCODING_CONFIG);
    switch (encoding) {
      case CLOUD_EVENT_BINARY_ENCODING:
        this.protobufSerializer = Protobuf.binarySerializer();
        break;
      case CLOUD_EVENT_STRUCTURED_ENCODING:
        this.protobufSerializer = Protobuf.structuredSerializer();
        break;
      default:
        throw new RuntimeException("Invalid cloudevent encoding: " + encoding);
    }

    this.topicManager = TopicManager.newBuilder()
        .setAdminClientProperties(config.clientProperties(AdminClientConfig.configNames()))
        .setDefaultTopicConfig(config.defaultTopicConfig())
        .setDefaultTopicPartitions(config.getInt(NonBlockingKafkaExporterConfig.TOPIC_PARTITIONS_CONFIG))
        .setDefaultTopicReplicas(config.getInt(NonBlockingKafkaExporterConfig.TOPIC_REPLICAS_CONFIG))
        .setTimeOutMs(config.getInt(NonBlockingKafkaExporterConfig.REQUEST_TIMEOUT_MS_CONFIG))
        .setTopics(config.getTopicSpecs())
        .build();

    // if the event logger is non-blocking, we need to keep the metadata fresh
    if (!config.getBoolean(EVENT_LOGGER_LOG_BLOCKING_CONFIG)) {
      metadataRefresh = new ScheduledThreadPoolExecutor(1);
      // To be safe, update this when it's half done
      ProducerConfig config = new ProducerConfig(producerProperties);
      long expiryMs = Math.min(
          config.getLong(CommonClientConfigs.METADATA_MAX_AGE_CONFIG),
          ProducerMetadata.TOPIC_EXPIRY_MS) / 2;
      metadataRefresh.scheduleAtFixedRate(this::ensureTopicsWithMetadata,
          expiryMs, expiryMs, TimeUnit.MILLISECONDS);
    }

    ensureTopicsWithMetadata();
  }

  @Override
  public void emit(CloudEvent<AttributesImpl, T> event) throws RuntimeException {
    try {
      // A default topic should have matched, even if no explicit routing is configured
      String topicName = route(event);

      // Make sure that the topic exists.
      boolean topicExists = this.topicManager.topicExists(topicName);

      if (!topicExists) {
        if (this.createTopic) {
          try {
            // Wait for all topics to be created.
            Future<Boolean> f = topicManager.ensureTopics();

            // The audit logs usecase has a "hard" non-blocking requirement. So, if this config is set,
            // queue the ensure topics task and throw an exception.
            if (config.getBoolean(EVENT_LOGGER_LOG_BLOCKING_CONFIG)) {
              boolean topicsReady = f.get(TOPIC_READY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
              if (topicsReady) {
                log.info("all topics created successfully");
              }
            } else {
              throw new RuntimeException("Topic " + topicName + " not found on cluster ["
                  + this.config.getString(BOOTSTRAP_SERVERS_CONFIG) + "]");
            }
          } catch (Exception e) {
            throw new RuntimeException("error while creating topics", e);
          }
        } else {
          throw new RuntimeException(
              "Topic " + topicName + " does not exist and topic creation is " +
                  "disabled. Cannot send event: " + protobufSerializer.toString(event));
        }
      }

      // producer may already be closed if we are shutting down
      if (!Thread.currentThread().isInterrupted() && !this.isClosing) {
        log.trace("Generated event log message : {}", event);
        this.producer.send(
            protobufSerializer.producerRecord(event, topicName, null),
            (metadata, exception) -> {
              if (exception != null) {
                exception.printStackTrace();
                log.error(
                    "Failed to produce event log message: " + protobufSerializer.toString(event));
              } else {
                log.debug(
                    "Produced event log message of size {} with offset {} to topic partition {}-{}",
                    metadata.serializedValueSize(),
                    metadata.offset(),
                    metadata.topic(),
                    metadata.partition()
                );
              }
            }
        );
      } else {
        log.warn(
            "Failed to produce event log message because audit logger is closing. Message: {}",
            protobufSerializer.toString(event));
      }

    } catch (InterruptException e) {
      // broker is shutting shutdown, interrupt flag is taken care of by InterruptException constructor
    } catch (Throwable t) {
      log.warn("Failed to produce event log message {}. Message: {}", t,
          protobufSerializer.toString(event));
    }
  }

  @Override
  public boolean routeReady(CloudEvent<AttributesImpl, T> event) {
    // This checks if the producer is ready to produce to this topic.

    String route = route(event);

    boolean topicExists = topicManager.topicExists(route);
    // Check if this topic exists. If not, ask the manager to create the topic if the topic
    // is managed by the topic manager.
    if (!topicExists && topicManager.topicManaged(route) && this.createTopic) {
      // Queue a reconcile job if it is not running (or queued) yet.
      topicManager.ensureTopics();
      // Dont block, wait for manager to reconcile in the background.
      return false;
    }

    // Only refresh metadata if the topic has been created.
    if (topicExists) {
      return metadataReady(route);
    }
    return false;
  }

  private boolean metadataReady(String topic) {
    try {
      // This makes a blocking metadata call. See {@link KafkaProducer#partitionsFor(String)}
      // But audit log code sets `max.block.ms = 0` to make it non-blocking.
      List<PartitionInfo> partitionInfo = producer.partitionsFor(topic);
      if (!partitionInfo.isEmpty()) {
        log.debug("Event log topic {} is ready with {} partitions", topic, partitionInfo.size());
        return true;
      }
    } catch (Exception e) {
      // We expect to get TimeoutExceptions here if the topic is not ready
      log.trace("Exception while checking for event log partitions", e);
    }
    log.debug("Event log topic {} is NOT ready", topic);
    return false;
  }

  private String route(CloudEvent<AttributesImpl, T> event) {
    if (event.getExtensions().containsKey(RouteExtension.Format.IN_MEMORY_KEY)) {
      RouteExtension re = (RouteExtension) event.getExtensions()
          .get(RouteExtension.Format.IN_MEMORY_KEY);
      if (!re.getRoute().isEmpty()) {
        return re.getRoute();
      }
    }
    return defaultRoute;
  }

  @Override
  public void close() throws Exception {
    isClosing = true;
    if (this.metadataRefresh != null) {
      this.metadataRefresh.shutdown();
    }
    if (this.producer != null) {
      this.producer.flush();
      this.producer.close(Duration.ofMillis(0));
    }
    if (this.topicManager != null) {
      this.topicManager.close();
    }
  }

  // The following methods define the dynamic configuration.
  @Override
  public Set<String> reconfigurableConfigs() {
    // Only allow overriding the topic spec.
    return Utils.mkSet(NonBlockingKafkaExporterConfig.TOPIC_CONFIG);
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    NonBlockingKafkaExporterConfig n = new NonBlockingKafkaExporterConfig(configs);
    // Parse the Topic JSON config to check if this is OK.
    n.getTopicSpecs();
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    // Add topics to the topic manager.
    NonBlockingKafkaExporterConfig n = new NonBlockingKafkaExporterConfig(configs);
    n.getTopicSpecs().values().stream().forEach(e -> this.topicManager.addTopic(e));

    ensureTopicsWithMetadata();
  }

  private void ensureTopicsWithMetadata() {
    if (this.createTopic) {
      try {
        // Wait for all topics to be created.
        Future<Boolean> f = topicManager.ensureTopics();
        boolean topicsReady = f.get(TOPIC_READY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (topicsReady) {
          log.info("all topics created successfully");
        }
      } catch (Exception e) {
        log.error("error while creating topics", e);
      }
    }

    // We want to make sure the producer is actually ready to write to these topics
    // If max.block.ms == 0, this might fail the first time, but should
    // succeed after a short wait
    Set<String> topicsWithoutMetadata = topicManager.managedTopics();
    topicsWithoutMetadata.removeIf(this::metadataReady);
    long waited = 0;
    while (!topicsWithoutMetadata.isEmpty() && waited < TOPIC_READY_TIMEOUT_MS) {
      log.info("Event logger is waiting for metadata for topics: " + topicsWithoutMetadata);
      try {
        waited += TOPIC_PARTITION_TIMEOUT_MS;
        Thread.sleep(TOPIC_PARTITION_TIMEOUT_MS);
      } catch (InterruptedException ignored) {
      }
      topicsWithoutMetadata.removeIf(this::metadataReady);
    }
    if (topicsWithoutMetadata.isEmpty()) {
      log.info("Event logger has metadata for all topics");
    } else {
      log.warn("Event logger is missing metadata for topics: " + topicsWithoutMetadata);
    }
    this.metadataRefreshed = Instant.now();
  }

  // Visibility for testing
  public Instant lastMetadataRefresh() {
    return this.metadataRefreshed;
  }


  // Exposed for testing.
  public NonBlockingKafkaExporterConfig config() {
    return this.config;
  }
}
