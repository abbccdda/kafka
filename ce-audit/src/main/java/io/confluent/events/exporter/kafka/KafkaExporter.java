/*
 * Copyright [2019 - 2019] Confluent Inc.
 */

package io.confluent.events.exporter.kafka;

import static io.confluent.events.EventLoggerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static io.confluent.events.EventLoggerConfig.CLOUD_EVENT_BINARY_ENCODING;
import static io.confluent.events.EventLoggerConfig.CLOUD_EVENT_ENCODING_CONFIG;
import static io.confluent.events.EventLoggerConfig.CLOUD_EVENT_STRUCTURED_ENCODING;
import static io.confluent.events.EventLoggerConfig.EVENT_LOGGER_LOG_BLOCKING_CONFIG;
import static io.confluent.events.cloudevents.kafka.Marshallers.binaryProto;
import static io.confluent.events.cloudevents.kafka.Marshallers.structuredProto;

import io.cloudevents.CloudEvent;
import io.cloudevents.format.Wire;
import io.cloudevents.format.builder.EventStep;
import io.cloudevents.v03.AttributesImpl;
import io.confluent.events.CloudEventUtils;
import io.confluent.events.EventLoggerConfig;
import io.confluent.events.cloudevents.extensions.RouteExtension;
import io.confluent.events.exporter.Exporter;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.ProducerMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The KafkaExporter sends events to topics based on their content
 */
public class KafkaExporter implements Exporter {

  private static final Logger log = LoggerFactory.getLogger(KafkaExporter.class);
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

  private EventLoggerConfig eventLogConfig;
  private EventStep<AttributesImpl, ? extends Object, byte[], byte[]> builder;

  public KafkaExporter() {
  }

  @Override
  public void configure(Map<String, ?> configs) {
    this.eventLogConfig = new EventLoggerConfig(configs);

    this.createTopic = eventLogConfig.getBoolean(EventLoggerConfig.TOPIC_CREATE_CONFIG);
    this.defaultRoute = EventLoggerConfig.DEFAULT_TOPIC;

    // TODO(sumit): Should payloads be configurable (or hardcoded to protobuf) ?
    this.producerProperties = eventLogConfig.producerProperties();
    this.producer = new KafkaProducer(this.producerProperties);

    if (eventLogConfig.getString(CLOUD_EVENT_ENCODING_CONFIG).equals(CLOUD_EVENT_BINARY_ENCODING)) {
      this.builder = binaryProto();
    } else if (eventLogConfig.getString(CLOUD_EVENT_ENCODING_CONFIG)
        .equals(CLOUD_EVENT_STRUCTURED_ENCODING)) {
      this.builder = structuredProto();
    } else {
      throw new RuntimeException(
          "Invalid cloudevent encoding: " + eventLogConfig.getString(CLOUD_EVENT_ENCODING_CONFIG));
    }

    this.topicManager = TopicManager.newBuilder()
        .setAdminClientProperties(eventLogConfig.clientProperties(AdminClientConfig.configNames()))
        .setDefaultTopicConfig(eventLogConfig.defaultTopicConfig())
        .setDefaultTopicPartitions(eventLogConfig.getInt(EventLoggerConfig.TOPIC_PARTITIONS_CONFIG))
        .setDefaultTopicReplicas(eventLogConfig.getInt(EventLoggerConfig.TOPIC_REPLICAS_CONFIG))
        .setTimeOutMs(eventLogConfig.getInt(EventLoggerConfig.REQUEST_TIMEOUT_MS_CONFIG))
        .setTopics(eventLogConfig.getTopicSpecs())
        .build();

    // if the event logger is non-blocking, we need to keep the metadata fresh
    if (!eventLogConfig.getBoolean(EVENT_LOGGER_LOG_BLOCKING_CONFIG)) {
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
  public void append(CloudEvent event) throws RuntimeException {
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
            if (eventLogConfig.getBoolean(EVENT_LOGGER_LOG_BLOCKING_CONFIG)) {
              boolean topicsReady = f.get(TOPIC_READY_TIMEOUT_MS, TimeUnit.MILLISECONDS);
              if (topicsReady) {
                log.info("all topics created successfully");
              }
            } else {
              throw new RuntimeException("Topic " + topicName + " not found on cluster ["
                  + this.eventLogConfig.getString(BOOTSTRAP_SERVERS_CONFIG) + "]");
            }
          } catch (Exception e) {
            throw new RuntimeException("error while creating topics", e);
          }
        } else {
          throw new RuntimeException(
              "Topic " + topicName + " does not exist and topic creation is " +
                  "disabled. Cannot send event: " + CloudEventUtils.toJsonString(event));
        }
      }

      // producer may already be closed if we are shutting down
      if (!Thread.currentThread().isInterrupted() && !this.isClosing) {
        log.trace("Generated event log message : {}", event);
        this.producer.send(
            marshal(event, builder, topicName, null),
            (metadata, exception) -> {
              if (exception != null) {
                exception.printStackTrace();
                log.error(
                    "Failed to produce event log message: " + CloudEventUtils.toJsonString(event));
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
            CloudEventUtils.toJsonString(event));
      }

    } catch (InterruptException e) {
      // broker is shutting shutdown, interrupt flag is taken care of by InterruptException constructor
    } catch (Throwable t) {
      log.warn("Failed to produce event log message {}. Message: {}", t,
          CloudEventUtils.toJsonString(event));
    }
  }

  @Override
  public boolean routeReady(CloudEvent event) {
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

  private String route(CloudEvent event) {
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
    return Utils.mkSet(EventLoggerConfig.TOPIC_CONFIG);
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    EventLoggerConfig n = new EventLoggerConfig(configs);
    // Parse the Topic JSON config to check if this is OK.
    n.getTopicSpecs();
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    // Add topics to the topic manager.
    EventLoggerConfig n = new EventLoggerConfig(configs);
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

  // The following code is copied from the Cloudevents SDK as the cloudevent producer wraps an older producer interface.
  private <T> Wire<byte[], String, byte[]> marshal(Supplier<CloudEvent<AttributesImpl, T>> event,
      EventStep<AttributesImpl, T, byte[], byte[]> builder) {

    return Optional.ofNullable(builder)
        .map(step -> step.withEvent(event))
        .map(marshaller -> marshaller.marshal())
        .get();

  }

  private Set<Header> marshal(Map<String, byte[]> headers) {

    return headers.entrySet()
        .stream()
        .map(header -> new RecordHeader(header.getKey(), header.getValue()))
        .collect(Collectors.toSet());

  }

  private <T> ProducerRecord<String, byte[]> marshal(CloudEvent<AttributesImpl, T> event,
      EventStep<AttributesImpl, T, byte[], byte[]> builder,
      String topic,
      Integer partition) {
    Wire<byte[], String, byte[]> wire = marshal(() -> event, builder);
    Set<Header> headers = marshal(wire.getHeaders());

    Long timestamp = null;
    if (event.getAttributes().getTime().isPresent()) {
      timestamp = event.getAttributes().getTime().get().toInstant().toEpochMilli();
    }

    if (!wire.getPayload().isPresent()) {
      throw new RuntimeException("payload is empty");
    }

    byte[] payload = wire
        .getPayload()
        .get();

    return new ProducerRecord<>(
        topic,
        partition,
        timestamp,
        // Get partitionKey from cloudevent extensions once it is supported upstream.
        null,
        payload,
        headers);
  }

  // Exposed for testing.
  public EventLoggerConfig eventLogConfig() {
    return this.eventLogConfig;
  }
}
