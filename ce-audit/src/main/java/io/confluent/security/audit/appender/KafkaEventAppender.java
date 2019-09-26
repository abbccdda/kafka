package io.confluent.security.audit.appender;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.CloudEvent;
import io.confluent.security.audit.CloudEventUtils;
import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.audit.router.AuditLogRouter;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig.Destinations;
import io.confluent.security.audit.router.EventTopicRouter;
import io.confluent.security.auth.utils.RetryBackoff;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The KafkaEventAppender sends events to topics based on their content
 */
public class KafkaEventAppender implements EventAppender {

  public static final int MAX_ATTEMPTS_PER_TOPIC = 10;
  private static final Logger log = LoggerFactory.getLogger(KafkaEventAppender.class);

  private boolean createTopic;
  private int topicReplicas;
  private int topicPartitions;
  private Map<String, String> topicConfig;
  private Properties producerProperties;
  private Properties adminClientProperties;
  private KafkaPrincipal eventLogPrincipal;
  private KafkaProducer<byte[], CloudEvent> producer;
  private EventTopicRouter topicRouter;

  public KafkaEventAppender() {

  }

  @Override
  public void configure(Map<String, ?> configs) {
    EventLogConfig eventLogConfig = new EventLogConfig(configs);

    this.createTopic = eventLogConfig.getBoolean(EventLogConfig.TOPIC_CREATE_CONFIG);
    this.topicReplicas = eventLogConfig.getInt(EventLogConfig.TOPIC_REPLICAS_CONFIG);
    this.topicPartitions = eventLogConfig.getInt(EventLogConfig.TOPIC_PARTITIONS_CONFIG);
    this.topicConfig = eventLogConfig.topicConfig();
    configureReconfigurable(eventLogConfig);
  }

  public void configureReconfigurable(EventLogConfig eventLogConfig) {
    // this may change because the bootstrap servers may change
    this.adminClientProperties = eventLogConfig.clientProperties();
    this.producerProperties = eventLogConfig.producerProperties();
    AuditLogRouterJsonConfig jsonConfig = eventLogConfig.routerJsonConfig();
    this.eventLogPrincipal = eventLogConfig.eventLogPrincipal();
    this.topicRouter = new AuditLogRouter(jsonConfig,
        eventLogConfig.getInt(EventLogConfig.ROUTER_CACHE_ENTRIES_CONFIG));
    this.producer = new KafkaProducer<>(this.producerProperties);

    Destinations destinations = jsonConfig.destinations;
    // make sure that all of the topics that we might route to exist
    if (!ensureTopics(destinations)) {
      throw new ConfigException("Could not create event log destination topics.");
    }

    RetryBackoff retryBackoff = new RetryBackoff(10, 100);
    // make sure the producer has metadata for all of the topics
    Set<String> topics = new HashSet<>(destinations.topics.keySet());
    try {
      int attempts = 0;
      int maxAttempts = topics.size() * MAX_ATTEMPTS_PER_TOPIC;
      while (!topics.isEmpty() && attempts < maxAttempts) {
        try {
          log.debug("Getting partitions for {} event log topics: {}", topics.size(), topics);
          topics.removeIf(t -> !this.producer.partitionsFor(t).isEmpty());
        } catch (TimeoutException e) {
          // retry until success
        }
        Thread.sleep(retryBackoff.backoffMs(attempts++));
      }
    } catch (InterruptedException e) {
      throw new ConfigException("Interrupted during event log configuration");
    }
    if (!topics.isEmpty()) {
      throw new ConfigException("Timed out waiting for event log topic metadata: " + topics);
    }
  }

  private boolean ensureTopics(Destinations destinations) {
    try (final AdminClient adminClient = AdminClient.create(this.adminClientProperties)) {
      // figure out which destination topics do not exist
      log.debug("ensuring that event log topics exist: {}", destinations.topics.keySet());
      Set<String> missing = new HashSet<>();
      for (Entry<String, KafkaFuture<TopicDescription>> topicFuture :
          adminClient.describeTopics(destinations.topics.keySet()).values().entrySet()) {
        try {
          topicFuture.getValue().get();
        } catch (ExecutionException e) {
          if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
            // something bad happened
            throw e;
          }
          missing.add(topicFuture.getKey());
        }
      }
      log.debug("{} event log topics need to be created: {}", missing.size(), missing);
      if (!createTopic) {
        return missing.isEmpty();
      }
      // create them
      List<NewTopic> newTopics = missing.stream().map(topicName -> {
        Map<String, String> config = new HashMap<>(this.topicConfig);
        config.put(TopicConfig.RETENTION_MS_CONFIG,
            String.valueOf(destinations.topics.get(topicName).retentionMs));
        return new NewTopic(topicName, this.topicPartitions, (short) this.topicReplicas)
            .configs(config);
      }).collect(Collectors.toList());
      adminClient.createTopics(newTopics).all().get();
      // make sure they actually got created
      DescribeTopicsResult describeResult = adminClient.describeTopics(missing);
      for (Entry<String, KafkaFuture<TopicDescription>> entry : describeResult.values().entrySet()) {
        TopicDescription description = entry.getValue().get();
        log.info("Event log topic {} created with {} partitions", entry.getKey(),
            description.partitions().size());
      }
    } catch (ExecutionException e) {
      log.error("Error checking or creating event log topic", e.getCause());
      return false;
    } catch (InterruptedException e) {
      log.warn("Confluent event log topic initialization interrupted");
      return false;
    }
    return true;
  }

  @Override
  public void append(CloudEvent event) throws RuntimeException {
    try {
      AuditLogEntry auditLogEntry = event.getData().unpack(AuditLogEntry.class);
      KafkaPrincipal eventPrincipal =
          SecurityUtils.parseKafkaPrincipal(
              auditLogEntry.getAuthenticationInfo().getPrincipal());
      if (eventLogPrincipal.equals(eventPrincipal)) {
        // suppress all events concerning the principal that is doing this logging
        // to make sure we don't loop infinitely
        return;
      }

      // A default route should have matched, even if no explicit routing is configured
      String topicName = this.topicRouter.topic(event).orElseThrow(
          () -> new ConfigException("No route configured"));

      // producer may already be closed if we are shutting down
      if (!Thread.currentThread().isInterrupted()) {
        log.trace("Generated event log message : {}", event);
        this.producer.send(
            new ProducerRecord<byte[], CloudEvent>(
                topicName,
                null,
                null,
                event
            ),
            new Callback() {
              @Override
              public void onCompletion(
                  RecordMetadata metadata,
                  Exception exception
              ) {
                if (exception != null) {
                  log.warn("Failed to produce event log message", exception);
                  try {
                    log.info(
                        "Failed to produce event log message: " + CloudEventUtils
                            .toJsonString(event));
                  } catch (InvalidProtocolBufferException e) {
                    log.warn("...and failed to log event that we couldn't produce", e);
                  }
                } else {
                  log.trace(
                      "Produced event log message of size {} with "
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
    } catch (InterruptException e) {
      // broker is shutting shutdown, interrupt flag is taken care of by
      // InterruptException constructor
    } catch (Throwable t) {
      log.warn("Failed to produce event log message", t);
      try {
        log.info("Failed to produce event log message: " + CloudEventUtils.toJsonString(event));
      } catch (InvalidProtocolBufferException e) {
        log.warn("...and failed to log event that we couldn't produce", e);
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (this.producer != null) {
      this.producer.close(Duration.ofMillis(0));
    }
  }

  @Override
  public Set<String> reconfigurableConfigs() {
    return Collections.singleton(EventLogConfig.ROUTER_CONFIG);
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {
    EventLogConfig config = new EventLogConfig(configs);
    try {
      AuditLogRouterJsonConfig
          .load(config.getString(EventLogConfig.ROUTER_CONFIG));
    } catch (IllegalArgumentException | IOException e) {
      throw new ConfigException(e.getMessage());
    }
  }

  @Override
  public void reconfigure(Map<String, ?> configs) {
    EventLogConfig eventLogConfig = new EventLogConfig(configs);

    configureReconfigurable(eventLogConfig);
  }
}
