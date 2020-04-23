/*
 * Copyright [2017  - 2017] Confluent Inc.
 */

package io.confluent.license;

import java.time.Duration;
import java.util.Collections;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TopicAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.confluent.command.record.Command.CommandConfigType;
import io.confluent.command.record.Command.CommandKey;
import io.confluent.command.record.Command.CommandMessage;
import io.confluent.command.record.Command.LicenseInfo;
import io.confluent.serializers.ProtoSerde;

/**
 * IMPORTANT: This class should not use Kafka APIs newer than AK 0.11.0/CP 3.3.0.
 *
 * All of our proprietary connectors are dependent upon the LicenseManager framework
 * and they all include this dependency in their plugin directory. So, when the connectors are
 * installed into any CP version, the connector's LicenseManager uses the AK APIs provided by the
 * Connect installation to create the license topic if necessary. Connectors may be installed into
 * CP 3.3.0 or newer when `AdminClient.createTopics(...)` was added. We should maintain
 * compatibility for the LicenseManager for all these versions by using only older APIs that are
 * available from 3.3.0.
 */
public class LicenseStore {

  private static final Logger log = LoggerFactory.getLogger(LicenseStore.class);
  private static final String KEY_PREFIX = "CONFLUENT_LICENSE";
  private static final CommandKey KEY = CommandKey.newBuilder()
      .setConfigType(CommandConfigType.LICENSE_INFO)
      .setGuid(KEY_PREFIX)
      .build();
  private static final Duration TOPIC_CREATE_RETRY_BACKOFF = Duration.ofMillis(1000);
  private final String topic;

  public static final String REPLICATION_FACTOR_CONFIG =
      "replication.factor";
  public static final String MIN_INSYNC_REPLICAS_CONFIG =
      "min.insync.replicas";
  public static final long READ_TO_END_TIMEOUT_MS = 120_000;

  private final KafkaBasedLog<CommandKey, CommandMessage> licenseLog;
  private final AtomicBoolean running = new AtomicBoolean();
  private final AtomicReference<String> latestLicense;
  private final Duration topicCreateTimeout;
  private final Time time;

  public LicenseStore(
      String topic,
      Map<String, Object> producerConfig,
      Map<String, Object> consumerConfig,
      Map<String, Object> topicConfig
  ) {
    this(topic, producerConfig, consumerConfig, topicConfig, Time.SYSTEM);
  }

  // visible for testing
  protected LicenseStore(
      String topic,
      Map<String, Object> producerConfig,
      Map<String, Object> consumerConfig,
      Map<String, Object> topicConfig,
      Time time
  ) {
    this(topic, producerConfig, consumerConfig, topicConfig, Duration.ZERO, Time.SYSTEM);
  }

  public LicenseStore(
      String topic,
      Map<String, Object> producerConfig,
      Map<String, Object> consumerConfig,
      Map<String, Object> topicConfig,
      Duration topicCreateTimeout,
      Time time
  ) {
    this.topic = topic;
    this.latestLicense = new AtomicReference<>();
    this.time = time;
    this.topicCreateTimeout = topicCreateTimeout;
    this.licenseLog = setupAndCreateKafkaBasedLog(
        this.topic,
        producerConfig,
        consumerConfig,
        topicConfig,
        this.latestLicense,
        this.time
    );
  }

  // visible for testing
  public LicenseStore(
      String topic,
      AtomicReference<String> latestLicense,
      KafkaBasedLog<CommandKey, CommandMessage> licenseLog,
      Time time
  ) {
    this.topic = topic;
    this.latestLicense = latestLicense;
    this.licenseLog = licenseLog;
    this.time = time;
    this.topicCreateTimeout = Duration.ZERO;
  }

  // package private for testing
  KafkaBasedLog<CommandKey, CommandMessage> setupAndCreateKafkaBasedLog(
      String topic,
      final Map<String, Object> producerConfig,
      final Map<String, Object> consumerConfig,
      final Map<String, Object> topicConfig,
      AtomicReference<String> latestLicense,
      Time time
  ) {
    Map<String, Object> producerProps = new HashMap<>();
    producerProps.putAll(producerConfig);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LicenseKeySerde.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        LicenseMessageSerde.class.getName()
    );
    producerProps.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

    Map<String, Object> consumerProps = new HashMap<>();
    consumerProps.putAll(consumerConfig);
    consumerProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        LicenseKeySerde.class.getName()
    );

    consumerProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        LicenseMessageSerde.class.getName()
    );

    String replicationFactorString =
        (String) topicConfig.get(REPLICATION_FACTOR_CONFIG);

    short replicationFactor = replicationFactorString == null
                              ? (short) 3
                              : Short.valueOf(replicationFactorString);
    String minInSyncReplicasString = (String) topicConfig.get(MIN_INSYNC_REPLICAS_CONFIG);
    short minInSyncReplicas = (short) Math.min(minInSyncReplicasString == null
                              ? 2
                              : Short.valueOf(minInSyncReplicasString), replicationFactor);


    NewTopic topicDescription = TopicAdmin.defineTopic(topic)
        .compacted()
        .partitions(1)
        .replicationFactor(replicationFactor)
        .minInSyncReplicas(minInSyncReplicas)
        .build();

    return createKafkaBasedLog(
        topic,
        producerProps,
        consumerProps,
        new ConsumeCallback(latestLicense),
        topicDescription,
        topicConfig,
        time
    );
  }

  private KafkaBasedLog<CommandKey, CommandMessage> createKafkaBasedLog(
      String topic,
      Map<String, Object> producerProps,
      Map<String, Object> consumerProps,
      Callback<ConsumerRecord<CommandKey, CommandMessage>> consumedCallback,
      final NewTopic topicDescription,
      final Map<String, Object> topicConfig,
      Time time
  ) {
    long endTimeMs = time.milliseconds() + topicCreateTimeout.toMillis();
    Runnable createTopics = new Runnable() {
      @Override
      public void run() {
        // NOTE: This uses AdminClient and not the new Admin API to ensure that connectors
        // using this framework can be installed into Connect from older CP versions.
        try (AdminClient admin = AdminClient.create(topicConfig)) {
          try {
            admin.describeTopics(Collections.singleton(topic)).all().get();
            log.debug("Topic {} already exists.", topic);
            return;
          } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnknownTopicOrPartitionException)
              log.debug("Topic {} does not exist, will attempt to create", topic);
            else if (cause instanceof RetriableException) {
              log.debug("Topic could not be described, will attempt to create", e);
            } else
              throw toKafkaException(cause);
          } catch (InterruptedException e) {
            throw new InterruptException(e);
          }

          Throwable lastException = null;
          while (true) {
            try {
              admin.createTopics(Collections.singleton(topicDescription)).all().get();
              log.debug("License topic {} created", topic);
              break;
            } catch (ExecutionException e) {
              Throwable cause = e.getCause();
              if (lastException == null) {
                if (cause instanceof InvalidReplicationFactorException) {
                  log.info("Creating topic {} with replication factor {}. " +
                          "At least {} brokers must be started concurrently to complete license registration.",
                      topic, topicDescription.replicationFactor(),
                      topicDescription.replicationFactor());
                } else if (cause instanceof UnsupportedVersionException) {
                  log.info("Topic {} could not be created due to UnsupportedVersionException. " +
                          "This may be indicate that a rolling upgrade from an older version is in progress. " +
                          "The request will be retried.", topic);
                }
              }
              lastException = cause;
              if (cause instanceof TopicExistsException) {
                log.debug("License topic {} was created by different node", topic);
                break;
              } else if (!(cause instanceof RetriableException ||
                  cause instanceof InvalidReplicationFactorException ||
                  cause instanceof UnsupportedVersionException)) {
                // Retry for:
                // 1) any retriable exception
                // 2) InvalidReplicationFactorException -  may indicate that brokers are starting up
                // 3) UnsupportedVersionException -  may be a rolling upgrade from a version older than
                //    0.10.1.0 before CreateTopics request was introduced
                throw toKafkaException(cause);
              }
            } catch (InterruptedException e) {
              throw new InterruptException(e);
            }

            long remainingMs = endTimeMs - time.milliseconds();
            if (remainingMs <= 0) {
              throw new org.apache.kafka.common.errors.TimeoutException("License topic could not be created", lastException);
            } else {
              log.debug("Topic could not be created, retrying: {}", lastException.getMessage());
              time.sleep(Math.min(remainingMs, TOPIC_CREATE_RETRY_BACKOFF.toMillis()));
            }
          }
        }
      }
    };
    return new KafkaBasedLog<>(
        topic,
        producerProps,
        consumerProps,
        consumedCallback,
        time,
        createTopics
    );
  }

  private KafkaException toKafkaException(Throwable t) {
    return t instanceof KafkaException ? (KafkaException) t : new KafkaException(t);
  }

  public static class LicenseKeySerde extends ProtoSerde<CommandKey> {
    public LicenseKeySerde() {
      super(CommandKey.getDefaultInstance());
    }
  }

  public static class LicenseMessageSerde extends ProtoSerde<CommandMessage> {
    public LicenseMessageSerde() {
      super(CommandMessage.getDefaultInstance());
    }
  }

  public void start() {
    if (running.compareAndSet(false, true)) {
      log.info("Starting License Store");
      startLog();
      log.info("Started License Store");
    }
  }

  public void stop() {
    if (running.compareAndSet(true, false)) {
      log.info("Closing License Store");
      stopLog();
      log.info("Closed License Store");
    }
  }

  protected void startLog() {
    licenseLog.start();
  }

  protected void stopLog() {
    licenseLog.stop();
  }

  public String licenseScan() {
    try {
      licenseLog.readToEnd().get(READ_TO_END_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      return latestLicense.get();
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      log.error("Failed to read license from Kafka: ", e);
      throw new IllegalStateException(e);
    }
  }

  public void registerLicense(String license) {
    registerLicense(license, null);
  }

  public void registerLicense(String license, org.apache.kafka.clients.producer.Callback callback) {
    CommandMessage licenseMsg = CommandMessage.newBuilder()
        .setLicenseInfo(LicenseInfo.newBuilder().setJwt(license).build())
        .build();
    licenseLog.send(KEY, licenseMsg, callback);
  }

  public static class ConsumeCallback implements
      Callback<ConsumerRecord<CommandKey, CommandMessage>> {
    private final AtomicReference<String> latestLicenseRef;

    ConsumeCallback(AtomicReference<String> latestLicenseRef) {
      this.latestLicenseRef = latestLicenseRef;
    }

    @Override
    public void onCompletion(Throwable error, ConsumerRecord<CommandKey, CommandMessage> record) {
      if (error != null) {
        log.error("Unexpected error in consumer callback for LicenseStore: ", error);
        return;
      }

      if (record.key().getConfigType() == CommandConfigType.LICENSE_INFO) {
        latestLicenseRef.set(record.value().getLicenseInfo().getJwt());
      }
    }
  }
}
