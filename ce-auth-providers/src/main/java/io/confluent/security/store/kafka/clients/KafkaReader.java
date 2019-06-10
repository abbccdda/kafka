// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.clients;

import io.confluent.security.authorizer.utils.ThreadUtils;
import io.confluent.security.store.KeyValueStore;
import io.confluent.security.store.MetadataStoreStatus;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaReader<K, V> implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(KafkaReader.class);

  private final String topic;
  private final int numPartitions;
  private final Consumer<K, V> consumer;
  private final KeyValueStore<K, V> cache;
  private final Time time;
  private final ExecutorService executor;
  private final AtomicBoolean alive;
  private final Map<TopicPartition, PartitionState> partitionStates;
  private final ConsumerListener<K, V> consumerListener;
  private final StatusListener statusListener;

  public KafkaReader(String topic,
                     int numPartitions,
                     Consumer<K, V> consumer,
                     KeyValueStore<K, V> cache,
                     ConsumerListener<K, V> consumerListener,
                     StatusListener statusListener,
                     Time time) {
    this.topic = Objects.requireNonNull(topic, "topic");
    this.numPartitions = numPartitions;
    this.consumer = Objects.requireNonNull(consumer, "consumer");
    this.cache = Objects.requireNonNull(cache, "cache");
    this.time = Objects.requireNonNull(time, "time");
    this.consumerListener = consumerListener;
    this.statusListener = statusListener;
    this.executor = Executors.newSingleThreadExecutor(
        ThreadUtils.createThreadFactory("auth-reader-%d", true));
    this.alive = new AtomicBoolean(true);
    this.partitionStates = new HashMap<>();
  }

  public CompletionStage<Void> start(Duration topicCreateTimeout) {
    CompletableFuture<Void> readyFuture = new CompletableFuture<>();
    executor.submit(() -> {
      try {
        initialize(topicCreateTimeout);
        List<CompletableFuture<Void>> futures = partitionStates.values().stream()
            .map(s -> s.readyFuture).collect(Collectors.toList());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()]))
            .whenComplete((result, exception) -> {
              if (exception != null) {
                log.error("Kafka reader failed to initialize partition", exception);
                readyFuture.completeExceptionally(exception);
              } else {
                log.debug("Kafka reader initialized on all partitions");
                readyFuture.complete(result);
              }
            });
      } catch (Throwable e) {
        log.error("Failed to initialize Kafka reader", e);
        alive.set(false);
        readyFuture.completeExceptionally(e);
      }
    });
    executor.submit(this);

    return readyFuture;
  }

  private void initialize(Duration topicCreateTimeout) {
    KafkaUtils.waitForTopic(topic,
        numPartitions,
        time,
        topicCreateTimeout,
        this::describeTopic,
        null);

    Set<TopicPartition> partitions = IntStream.range(0, numPartitions)
        .mapToObj(p -> new TopicPartition(topic, p))
        .collect(Collectors.toSet());
    this.consumer.assign(partitions);

    consumer.seekToEnd(partitions);
    partitions
        .forEach(tp -> partitionStates.put(tp, new PartitionState(consumer.position(tp) - 1)));

    log.debug("auth topic partitions : {}", partitions);
    consumer.seekToBeginning(partitions);
  }

  private Set<Integer> describeTopic(String topic) {
    if (!alive.get())
      throw new RuntimeException("KafkaReader has been shutdown");
    List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
    if (partitionInfos != null)
      return partitionInfos.stream().map(PartitionInfo::partition).collect(Collectors.toSet());
    else
      return Collections.emptySet();
  }

  @Override
  public void run() {
    while (alive.get()) {
      try {
        ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
        try {
          records.forEach(this::processConsumerRecord);
          statusListener.onReaderSuccess();
        } catch (Exception e) {
          // Since failure to process record could result in granting incorrect access to users,
          // treat this as a fatal exception and deny all access.
          log.error("Unexpected exception while processing records {}", records, e);
          fail(e);
          break;
        }
      } catch (WakeupException e) {
        log.trace("Wakeup exception, consumer may be closing", e);
      } catch (Throwable e) {
        log.error("Unexpected exception from consumer poll", e);
        if (statusListener.onReaderFailure()) {
          fail(e);
          break;
        }
      }
    }
  }

  // For unit tests
  int numPartitions() {
    return partitionStates.size();
  }

  public void close(Duration closeTimeout) {
    if (alive.getAndSet(false)) {
      consumer.wakeup();
      executor.shutdownNow();
    }
    long endMs = time.milliseconds();
    try {
      executor.awaitTermination(closeTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      log.debug("KafkaReader was interrupted while waiting to close");
      throw new InterruptException(e);
    }
    long remainingMs = Math.max(0, endMs - time.milliseconds());
    consumer.close(Duration.ofMillis(remainingMs));
  }

  private void processConsumerRecord(ConsumerRecord<K, V> record) {
    K key = record.key();
    V newValue = record.value();
    V oldValue;
    if (newValue != null)
      oldValue = cache.put(key, newValue);
    else
      oldValue = cache.remove(key);
    log.debug("Processing new record {}-{}:{} key {} newValue {} oldValue {}",
        record.topic(), record.partition(), record.offset(), key, newValue, oldValue);
    if (consumerListener != null)
      consumerListener.onConsumerRecord(record, oldValue);
    PartitionState partitionState = partitionStates.get(new TopicPartition(record.topic(), record.partition()));
    if (partitionState != null) {
      partitionState.onConsume(record.offset(), cache.status(record.partition()) == MetadataStoreStatus.INITIALIZED);
    }
  }

  private void fail(Throwable e) {
    IntStream.range(0, numPartitions).forEach(p -> cache.fail(p, "Metadata reader failed with exception: " + e));
  }

  private static class PartitionState {
    private final long minOffset;
    private final CompletableFuture<Void> readyFuture;
    volatile long currentOffset;

    PartitionState(long offsetAtStartup) {
      this.minOffset = offsetAtStartup;
      this.readyFuture = new CompletableFuture<>();
    }

    void onConsume(long offset, boolean initialized) {
      this.currentOffset = offset;

      if (!readyFuture.isDone() && currentOffset >= minOffset && initialized)
        readyFuture.complete(null);
    }

    @Override
    public String toString() {
      return "PartitionState(" +
          "minOffset=" + minOffset +
          ", currentOffset=" + currentOffset +
          ", ready=" + readyFuture.isDone() +
          ')';
    }
  }
}
