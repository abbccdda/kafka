package io.confluent.security.audit.appender;

import com.google.protobuf.InvalidProtocolBufferException;
import io.confluent.security.audit.AuditLogEntry;
import io.confluent.security.audit.CloudEvent;
import io.confluent.security.audit.CloudEventUtils;
import io.confluent.security.audit.EventLogConfig;
import io.confluent.security.audit.router.AuditLogRouter;
import io.confluent.security.audit.router.EventTopicRouter;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.SecurityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The KafkaEventAppender sends events to topics based on their content
 */
public class KafkaEventAppender implements EventAppender {

  private static final Logger log = LoggerFactory.getLogger(KafkaEventAppender.class);

  private ConcurrentHashMap<String, Boolean> isTopicCreated = new ConcurrentHashMap<>();
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
    this.producerProperties = eventLogConfig.producerProperties();
    this.adminClientProperties = eventLogConfig.clientProperties();
    this.eventLogPrincipal = eventLogConfig.eventLogPrincipal();
    this.topicRouter = new AuditLogRouter(eventLogConfig.routerJsonConfig(),
        eventLogConfig.getInt(EventLogConfig.ROUTER_CACHE_ENTRIES_CONFIG));
    this.producer = new KafkaProducer<>(this.producerProperties);
  }

  public boolean ensureTopic(String topicName) {

    try (final AdminClient adminClient = AdminClient.create(this.adminClientProperties)) {
      try {
        adminClient.describeTopics(Collections.singleton(topicName)).all().get();
        log.debug("Event log topic {} already exists", topicName);
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
          // something bad happened
          throw e;
        }

        adminClient
            .createTopics(
                Collections.singleton(new NewTopic(
                    topicName,
                    this.topicPartitions,
                    (short) this.topicReplicas
                ).configs(this.topicConfig))
            )
            .all()
            .get();
        log.info("Created event log topic {}", topicName);
      }
      return true;
    } catch (ExecutionException e) {
      log.error("Error checking or creating event log topic", e.getCause());
      return false;
    } catch (InterruptedException e) {
      log.warn("Confluent event log topic initialization interrupted");
      return false;
    }
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

      if (this.createTopic) {
        if (!this.isTopicCreated.getOrDefault(topicName, false)) {
          this.isTopicCreated.put(topicName, ensureTopic(topicName));
        }
        // if topic can't be created, skip the rest
        if (!this.isTopicCreated.getOrDefault(topicName, false)) {
          log.warn("Failed to create Event Log topic: " + topicName);
          return;
        }
      }
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
    // When we route messages based on the content, this will be reconfigurable
    return null;
  }

  @Override
  public void validateReconfiguration(Map<String, ?> configs) throws ConfigException {

  }

  @Override
  public void reconfigure(Map<String, ?> configs) {

  }
}
