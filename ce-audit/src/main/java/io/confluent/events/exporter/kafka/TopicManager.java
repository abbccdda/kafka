/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.events.exporter.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Ensure that the list of topics are created to match config.
public class TopicManager implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(TopicManager.class);

  private final ConcurrentHashMap<String, Boolean> topicReady;
  private final ConcurrentHashMap<String, TopicSpec> topicMap;

  private final ThreadPoolExecutor reconcileJobExecutor;
  private final AdminClient adminClient;
  private final Map<String, String> defaultTopicConfig;
  private final Integer defaultTopicPartitions;
  private final Integer defaultTopicReplicas;
  private final Integer timeOutMs;
  private Future<Boolean> reconcileFuture;


  public TopicManager(Properties clientProps,
      Map<String, String> defaultTopicConfig,
      Integer defaultTopicPartitions,
      Integer defaultTopicReplicas,
      Integer timeOutMs,
      Map<String, TopicSpec> topics) {

    this.adminClient = AdminClient.create(clientProps);
    this.defaultTopicConfig = defaultTopicConfig;
    this.defaultTopicPartitions = defaultTopicPartitions;
    this.defaultTopicReplicas = defaultTopicReplicas;

    this.timeOutMs = timeOutMs;

    // Since this executor does only one thing, it needn't support a backlog
    reconcileJobExecutor = new ThreadPoolExecutor(
        1,
        1,
        30, TimeUnit.SECONDS,
        new ArrayBlockingQueue<>(1),
        new ThreadPoolExecutor.AbortPolicy());

    reconcileJobExecutor.setThreadFactory(runnable -> {
      Thread thread = new Thread(runnable, "confluent-event-topic-manager-task-scheduler");
      thread.setDaemon(true);
      thread.setUncaughtExceptionHandler(
          (t, e) -> log.error("Uncaught exception in thread '{}':", t.getName(), e));
      return thread;
    });

    // We probably create our topics at startup, so we don't need to keep the thread around
    reconcileJobExecutor.allowCoreThreadTimeOut(true);

    topicReady = new ConcurrentHashMap<>();
    topicMap = new ConcurrentHashMap<>();

    topics.entrySet().stream().forEach(t -> {
      topicMap.put(t.getKey(), t.getValue());
      topicReady.put(t.getKey(), false);
    });

  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public Future<Boolean> ensureTopics() {

    // add a task to the executor if there is not one running already.
    // If one is running already, then the executor will reject the task. If that happens, return the reconcileFuture
    // to the task already in the queue.
    try {
      // TODO(sumit): Use delayed task so that we don't overwhelm the broker if the reconcile requests fails quickly.
      this.reconcileFuture = reconcileJobExecutor.submit(this::reconcile);
    } catch (RejectedExecutionException e) {
    }
    return this.reconcileFuture;
  }

  private void mergeDefaults(TopicSpec spec) {

    if (spec.partitions() <= 0) {
      spec.setPartitions(this.defaultTopicPartitions);
    }

    if (spec.replicationFactor() <= 0) {
      spec.setReplicationFactor(this.defaultTopicReplicas);
    }

    if (this.defaultTopicConfig != null) {
      this.defaultTopicConfig.entrySet().stream()
          .forEach(e -> spec.config().putIfAbsent(e.getKey(), e.getValue()));
    }
  }

  public boolean reconcile() {
    try {

      // Verify that the topics are not already there.
      // It is possible that this AdminClient has permission to DescribeTopics, but not CreateTopics.
      // If something else has created topics that we don't have permission to create, we'd like this
      // reconcile job to notice that and update topicReady
      DescribeTopicsResult describeResult = adminClient.describeTopics(topicMap.keySet(),
          new DescribeTopicsOptions().timeoutMs(timeOutMs));
      for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : describeResult.values()
          .entrySet()) {
        try {
          TopicDescription description = entry.getValue().get(timeOutMs, TimeUnit.MILLISECONDS);
          topicReady.put(description.name(), true);
          log.debug("Event log topic {} ready with {} partitions", entry.getKey(),
              description.partitions().size());
        } catch (ExecutionException | TimeoutException e) {
          if (e.getCause() instanceof UnknownTopicOrPartitionException) {
            topicReady.put(entry.getKey(), false);
          } else {
            // something bad happened
            log.error("error while describing topics", e);
          }
        }
      }

      Set<String> unready = topicReady.entrySet().stream()
          .filter(e -> !e.getValue())
          .map(Map.Entry::getKey)
          .collect(Collectors.toSet());

      // create if some topics are still not ready
      List<NewTopic> newTopics = unready.stream().map(topicName -> {
        TopicSpec ts = topicMap.get(topicName);
        mergeDefaults(ts);
        return new NewTopic(topicName, ts.partitions(), (short) ts.replicationFactor())
            .configs(ts.config());
      }).collect(Collectors.toList());

      CreateTopicsResult createResult = adminClient
          .createTopics(newTopics, new CreateTopicsOptions().timeoutMs(timeOutMs));
      for (Map.Entry<String, KafkaFuture<Void>> entry : createResult.values().entrySet()) {
        try {
          entry.getValue().get(timeOutMs, TimeUnit.MILLISECONDS);
          topicReady.put(entry.getKey(), true);
        } catch (ExecutionException | TimeoutException e) {
          if (e.getCause() instanceof TopicExistsException) {
            topicReady.put(entry.getKey(), true);
          } else {
            // Ignore if the topic already exists, otherwise this is a problem
            log.error("error while creating topics", e);
          }
        }
      }
    } catch (InterruptedException e) {
      log.warn("Event log topic initialization interrupted");
    }

    return topicReady.values().stream().allMatch(v -> v);
  }

  public void addTopic(TopicSpec spec) {
    this.topicMap.put(spec.name(), spec);
  }

  public boolean topicExists(String name) {
    return topicReady.containsKey(name);
  }

  public boolean topicManaged(String name) {
    return topicMap.containsKey(name);
  }

  @Override
  public void close() throws IOException {
    this.reconcileJobExecutor.shutdownNow();
    this.adminClient.close();
  }

  public static final class Builder {

    private Properties adminClientProperties;
    private Integer defaultTopicPartitions;
    private Integer defaultTopicReplicas;
    private Map<String, String> defaultTopicConfig = new HashMap<>();
    private Integer timeOutMs;
    private Map<String, TopicSpec> topics;

    private Builder() {
    }

    public Builder setDefaultTopicPartitions(Integer defaultTopicPartitions) {
      this.defaultTopicPartitions = defaultTopicPartitions;
      return this;
    }

    public Builder setDefaultTopicReplicas(Integer defaultTopicReplicas) {
      this.defaultTopicReplicas = defaultTopicReplicas;
      return this;
    }

    public Builder setAdminClientProperties(Properties adminClientProperties) {
      this.adminClientProperties = adminClientProperties;
      return this;
    }

    public Builder setDefaultTopicConfig(Map<String, String> defaultTopicConfig) {
      this.defaultTopicConfig = defaultTopicConfig;
      return this;
    }

    public Builder setTimeOutMs(Integer timeOutMs) {
      this.timeOutMs = timeOutMs;
      return this;
    }

    public Builder setTopics(Map<String, TopicSpec> topics) {
      this.topics = topics;
      return this;
    }

    public TopicManager build() {
      Objects.requireNonNull(this.adminClientProperties, "Admin client properties is required.");
      Objects.requireNonNull(this.timeOutMs, "timeout is required");
      return new TopicManager(this.adminClientProperties,
          this.defaultTopicConfig,
          this.defaultTopicPartitions,
          this.defaultTopicReplicas,
          this.timeOutMs,
          this.topics);
    }
  }

}
