package io.confluent.security.audit.appender;

import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.CloudEvent;
import io.confluent.security.audit.CloudEventUtils;
import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.audit.router.AuditLogRouter;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig;
import io.confluent.security.audit.router.AuditLogRouterJsonConfig.Destinations;
import io.confluent.security.audit.router.EventTopicRouter;
import io.confluent.security.auth.utils.RetryBackoff;
import io.confluent.security.authorizer.utils.ThreadUtils;
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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TopicExistsException;
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
  public static final int MAX_WAIT_FOR_CREATION_MS = 10000;
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
  private Destinations destinations;
  private ConcurrentHashMap<String, Boolean> topicReady = new ConcurrentHashMap<>();
  private ThreadPoolExecutor createDestinationTopicsExecutor;

  public KafkaEventAppender() {
    // Since this executor does only one thing, it needn't support a backlog
    createDestinationTopicsExecutor = new ThreadPoolExecutor(1, 1,
        30, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(1),
        ThreadUtils.createThreadFactory("kafka-event-appender-%d", true),
        new ThreadPoolExecutor.DiscardPolicy());
    // We probably create our topics at startup, so we don't need to keep the thread around
    createDestinationTopicsExecutor.allowCoreThreadTimeOut(true);
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

    topicReady = new ConcurrentHashMap<>();
    this.destinations = jsonConfig.destinations;
    // make sure that all of the topics that we might route to exist
    Future<Boolean> future = createDestinationTopicsExecutor.submit(this::createDestinationTopics);
    boolean created = false;
    try {
      created = future.get(MAX_WAIT_FOR_CREATION_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      // Ignore hopefully transient errors
    }
    if (!created) {
      log.warn(
          "Could not create some event log destination topics. Will retry when they are logged to");
    } else {
      // If we succeeded in creating the topics, wait for them to be ready
      waitForCreatedTopics();
    }
  }

  private void waitForCreatedTopics() {
    RetryBackoff retryBackoff = new RetryBackoff(10, 100);
    Set<String> unready = unreadyTopics();
    try {
      int attempts = 0;
      int maxAttempts = unready.size() * MAX_ATTEMPTS_PER_TOPIC;
      while (!unready.isEmpty() && attempts < maxAttempts) {
        log.debug("Waiting {} more times for {} event log topics to be ready",
            maxAttempts - attempts, unready.size());
        Thread.sleep(retryBackoff.backoffMs(attempts++));
        unready = unreadyTopics();
      }
    } catch (InterruptedException e) {
      throw new ConfigException("Interrupted during event log configuration");
    }
    if (!unready.isEmpty()) {
      log.warn("Timed out waiting for event log topic metadata: " + unready);
    } else {
      log.debug("All event log topics are ready");
    }
  }

  /*
   * The topics that the producer doesn't have metadata for
   */
  private Set<String> unreadyTopics() {
    Set<String> unready = destinations.topics.keySet().stream()
        .filter(t -> !topicReady.getOrDefault(t, false))
        .collect(Collectors.toSet());
    if (unready.isEmpty()) {
      return unready;
    }
    for (String topic : unready) {
      List<PartitionInfo> partitionInfo = Collections.emptyList();
      try {
        partitionInfo = producer.partitionsFor(topic);
      } catch (Exception e) {
        // We expect to get TimeoutExceptions here if the topic is not ready
        log.trace("Exception while checking for event log partitions", e);
      }
      if (partitionInfo.isEmpty()) {
        log.debug("Event log topic {} not ready", topic);
      } else {
        log.info("Event log topic {} is ready with {} partitions", topic, partitionInfo.size());
        topicReady.put(topic, true);
      }
    }
    return destinations.topics.keySet().stream()
        .filter(t -> !topicReady.getOrDefault(t, false))
        .collect(Collectors.toSet());
  }

  @Override
  public boolean ready() {
    return destinations != null && unreadyTopics().isEmpty();
  }

  /*
   * Attempt to create the topics if they don't already exist. Returns true if no errors were encountered.
   * The topics might still not be ready after this runs, because the producer might still not
   * have metadata for them.
   */
  private boolean createDestinationTopics() {
    log.debug("Ensuring that event log topics exist");
    Set<String> unready = unreadyTopics();
    if (unready.isEmpty()) {
      log.debug("All event log topics already exist");
      return true;
    }
    try (final AdminClient adminClient = AdminClient.create(this.adminClientProperties)) {
      // figure out which destination topics do not exist (vs existing, but producer doesn't
      // have metadata yet)
      Set<String> toCreate = new HashSet<>();
      DescribeTopicsResult describeResult = adminClient.describeTopics(unready);
      for (Entry<String, KafkaFuture<TopicDescription>> entry : describeResult.values()
          .entrySet()) {
        try {
          TopicDescription description = entry.getValue().get();
          log.debug("Event log topic {} ready with {} partitions", entry.getKey(),
              description.partitions().size());
        } catch (ExecutionException e) {
          if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
            // something bad happened
            throw e;
          }
          toCreate.add(entry.getKey());
          topicReady.put(entry.getKey(), false);
          log.debug("Event log topic {} not ready", entry.getKey());
        }
      }
      log.debug("{} event log topics need to be created: {}", toCreate.size(), toCreate);
      if (!createTopic) {
        return toCreate.isEmpty();
      }
      // create them
      List<NewTopic> newTopics = toCreate.stream().map(topicName -> {
        Map<String, String> config = new HashMap<>(this.topicConfig);
        config.put(TopicConfig.RETENTION_MS_CONFIG,
            String.valueOf(destinations.topics.get(topicName).retentionMs));
        return new NewTopic(topicName, this.topicPartitions, (short) this.topicReplicas)
            .configs(config);
      }).collect(Collectors.toList());
      for (Entry<String, KafkaFuture<Void>> entry :
          adminClient.createTopics(newTopics).values().entrySet()) {
        try {
          entry.getValue().get();
        } catch (ExecutionException e) {
          if (!(e.getCause() instanceof TopicExistsException)) {
            // Ignore if the topic already exists, otherwise this is a problem
            throw e;
          }
        }
      }
    } catch (ExecutionException e) {
      log.error("Error checking or creating event log topic", e.getCause());
      return false;
    } catch (InterruptedException e) {
      log.warn("Event log topic initialization interrupted");
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
        log.debug("Suppressed event log message from the event log principal: " + eventPrincipal);
        return;
      }

      // A default route should have matched, even if no explicit routing is configured
      String topicName = this.topicRouter.topic(event).orElseThrow(
          () -> new ConfigException("No route configured"));

      if (topicName.isEmpty()) {
        return;
      }

      // If we can't write to this topic yet, log to file
      if (!topicReady.getOrDefault(topicName, false)) {
        // Try creating topics asynchronously, in case we now have permission
        createDestinationTopicsExecutor.submit(this::createDestinationTopics);
        log.info("Topic {} not ready for event log message: {}",
            topicName, CloudEventUtils.toJsonString(event));
        return;
      }

      // producer may already be closed if we are shutting down
      if (!Thread.currentThread().isInterrupted()) {
        if (log.isTraceEnabled()) {
          log.trace("Generated event log message : {}", CloudEventUtils.toPrettyJsonString(event));
        }
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
                  } catch (IOException e) {
                    log.warn("...and failed to log event that we couldn't produce", e);
                  }
                } else {
                  if (log.isTraceEnabled()) {
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
      } catch (IOException e) {
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
