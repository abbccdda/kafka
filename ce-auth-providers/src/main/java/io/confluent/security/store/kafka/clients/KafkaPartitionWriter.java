// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.clients;

import io.confluent.security.store.MetadataStoreStatus;
import io.confluent.security.store.KeyValueStore;
import io.confluent.security.store.NotMasterWriterException;
import io.confluent.security.store.kafka.coordinator.MetadataServiceRebalanceListener;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

/**
 * Writer for one metadata topic partition that encapsulates all the state associated with
 * the partition including master writer generation. The producer instance is shared across
 * all partition writers.
 * <p>
 * <b>Thread-safety:</b>
 * Operations of the partition writer are synchronized on the writer lock to ensure
 * that update operations to a partition are ordered. Producer is configured with
 * max.in.flight.requests.per.connection=1 to ensure that updates are ordered within the partition
 * even if there are retries. A single active writer makes updates to partitions at any one time.
 * <p>
 * All produce callbacks are ordered and invoked on the single producer network thread. All
 * consumer records from the reader are ordered and processed on a single reader thead. No ordering
 * can be assumed between the producer callback and the consumer record processing sequence.
 * </p>
 */
public class KafkaPartitionWriter<K, V> {

  private static final int NOT_MASTER_WRITER = -1;
  private static final int MAX_PENDING_WRITES = 100;

  private final Logger log;
  private final TopicPartition topicPartition;
  private final Producer<K, V> producer;
  private final KeyValueStore<K, V> cache;
  private final MetadataServiceRebalanceListener rebalanceListener;
  private final Duration refreshTimeout;
  private final Time time;
  private final BlockingQueue<PendingWrite> pendingWrites;

  private MetadataStoreStatus status;
  private int generationId;
  private long lastProducedOffset;
  private long lastConsumedOffset;

  public KafkaPartitionWriter(TopicPartition topicPartition,
                              Producer<K, V> producer,
                              KeyValueStore<K, V> cache,
                              MetadataServiceRebalanceListener rebalanceListener,
                              Duration refreshTimeout,
                              Time time) {
    this.topicPartition = topicPartition;
    this.producer = producer;
    this.cache = cache;
    this.rebalanceListener = rebalanceListener;
    this.refreshTimeout = refreshTimeout;
    this.time = time;
    this.generationId = NOT_MASTER_WRITER;
    this.status = MetadataStoreStatus.UNKNOWN;
    pendingWrites = new ArrayBlockingQueue<>(MAX_PENDING_WRITES);
    LogContext logContext = new LogContext("[PartitionWriter " + topicPartition + "]");
    log = logContext.logger(KafkaPartitionWriter.class);
  }

  /**
   * Starts this partition writer with the provided generation id. A status record with generation id
   * is created with the provided key/value and written to the partition. All records following the
   * status record are managed by this writer. Any unexpected records that appear after this
   * status, but before a new generation status will be undone by writing an updated record.
   *
   * This method is asynchronous and returns immediately after adding the generation status
   * to the producer. Subsequent updates will be blocked until the status is written to log
   * and consumed by the cache on this node.
   */
  public void start(final int generationId, K statusKey, V statusValue) {
    log.debug("Starting generation {} for partition writer {}", generationId, topicPartition);

    synchronized (this) {
      status(MetadataStoreStatus.INITIALIZING);
      this.generationId = generationId;
    }

    writeStatus(generationId, statusKey, statusValue, MetadataStoreStatus.INITIALIZING);
  }

  public void writeStatus(int generationId, K statusKey, V statusValue, MetadataStoreStatus status) {
    log.debug("writeStatus generation {} status {}", generationId, statusValue);
    synchronized (this) {
      if (this.generationId != generationId)
        return;
    }
    ProducerRecord<K, V> record = new ProducerRecord<>(topicPartition.topic(),
        topicPartition.partition(), statusKey, statusValue);
    producer.send(record, (metadata, exception) -> {
      if (exception != null) {
        log.error("Status {}:{} could not be added to auth topic", statusKey, statusValue, exception);
        rebalanceListener.onWriterResigned(generationId);
      } else {
        onStatusRecordWriteCompletion(generationId, status, metadata.offset());
      }
    });
  }

  /**
   * Stops this writer in preparation for rebalance. Pending writes that have not been completed
   * by the time the first status record is received from the new writer will be cancelled.
   */
  public synchronized void stop(Integer stoppingGeneration) {
    log.debug("Stop generation {} for partition writer {}", generationId, topicPartition);
    if (stoppingGeneration == null || stoppingGeneration == generationId) {
      this.generationId = NOT_MASTER_WRITER;
      this.status(MetadataStoreStatus.UNKNOWN);
    }
  }

  /**
   * Writes a record to the partition, blocking if necessary until this writer is ready and
   * the number of pending writes is within the limit. Write is only attempted if the record
   * can be added within refresh timeout. If a rebalance occurs within this time, the write is
   * aborted and {@link NotMasterWriterException} is thrown.
   *
   * @param key Key for the record, which must be non-null
   * @param value Value for the record, which may be null if a record is being deleted
   * @param expectedGenerationId Generation id corresponding to the write if this is an incremental
   *        update or value override
   *
   * @return future that completes when the record is written to the partition and consumed
   *         by the local reader
   */
  public CompletionStage<Void> write(K key, V value, Integer expectedGenerationId, boolean waitForInitialization) {
    PendingWrite pendingWrite;
    synchronized (this) {
      if (expectedGenerationId != null && this.generationId != expectedGenerationId) {
        throw notMasterWriterException();
      }

      pendingWrite = new PendingWrite(generationId, key);
      boolean canAdd = waitUntil(unused -> (!waitForInitialization || status == MetadataStoreStatus.INITIALIZED) &&
          pendingWrites.offer(pendingWrite), true);
      if (!canAdd)
        throw new TimeoutException("Failed to write record within timeout");
    }

    log.debug("Writing new record with key {} to partition {} generation id {}", key, topicPartition, expectedGenerationId);
    ProducerRecord<K, V> record = new ProducerRecord<>(topicPartition.topic(), topicPartition.partition(),
          key, value);
    try {
      producer.send(record, pendingWrite);
    } catch (Exception e) {
      onRecordWriteFailure(pendingWrite, e);
    }
    return pendingWrite.future;
  }

  /**
   * Waits for all pending writes to be flushed and available on the cache of the local reader.
   * This is used to obtain the latest value corresponding to the provided key before an
   * incremental update.
   *
   * @param key Key that is being updated
   * @return The latest record corresponding to the key from the cache
   * @throws TimeoutException if pending writes were not flushed and refreshed within timeout
   */
  public synchronized CachedRecord<K, V> waitForRefresh(K key) {
    if (waitUntil(unused -> pendingWrites.isEmpty() && status == MetadataStoreStatus.INITIALIZED, true))
      return new CachedRecord<>(key, cache.get(key), generationId);
    else
      throw new TimeoutException("Timed out waiting for pending writes to be completed and refreshed");
  }

  /**
   * Notification of writer status read by the local reader from this partition.
   * This is invoked on the reader's consumer polling thread when a record is received,
   * guaranteeing ordering between status and other records. When status record of a new genaration
   * is processed, any pending write with offset less than the offset is completed and any pending
   * write belonging to older generation is cancelled.
   *
   * @param newGenerationId Generation id from the status record
   * @param offset Offset of status record
   */
  public void onStatusConsumed(long offset, int newGenerationId, MetadataStoreStatus status) {
    log.debug("Received new generation id {} for partition writer {} at offset {} status {} generation {}",
        newGenerationId, topicPartition, offset, status, generationId);

    Integer resignGenerationId = null;

    synchronized (this) {
      this.lastConsumedOffset = offset;
      if (newGenerationId != -1 && newGenerationId == this.generationId && !waitUntilPendingOffsetsKnown(offset))
        resignGenerationId = generationId;

      maybeCompletePendingWrites(lastConsumedOffset);
      maybeCancelPendingWrites(newGenerationId);

      if (newGenerationId == this.generationId) {
        // Set status and notify any threads waiting to write
        this.status(status);
      } else if (newGenerationId > this.generationId && this.generationId != NOT_MASTER_WRITER) {

        // Received a newer generation id than that of this writer, so it must be from another node.
        // Ensure all pending writes have been cancelled and resign.
        log.debug("Received newer generation id, resigning");
        resignGenerationId = this.generationId;
        if (!pendingWrites.isEmpty())
          throw new IllegalStateException("All pending writes of older generation must have been cancelled");
      }
    }
    if (resignGenerationId != null)
      rebalanceListener.onWriterResigned(resignGenerationId);
  }

  /**
   * Notification of record consumed by the local reader. The local cache is populated
   * before this method is invoked. This method is invoked on the reader's consumer polling thread,
   * guaranteeing ordering between data records and status records. All pending writes with offset less
   * than or equal to this consumed offset are completed.
   */
  public void onRecordConsumed(ConsumerRecord<K, V> record, V oldValue, boolean expectPendingWriteOnMaster) {
    Integer resignGenerationId = null;
    boolean overwriteValue = false;

    synchronized (this) {
      long offset = record.offset();

      if (!waitUntilPendingOffsetsKnown(offset)) {
        resignGenerationId = generationId;
      } else if (expectPendingWriteOnMaster && status == MetadataStoreStatus.INITIALIZED && !pendingWriteExists(offset)) {

        // If we are not the master or haven't yet consumed our generation status, then
        // we are not in ready state. In this case, we simply consume the record.
        // If we are ready and see an update that we didn't send, overwrite with our value.
        overwriteValue = true;
      }
      this.lastConsumedOffset = offset;
      maybeCompletePendingWrites(lastConsumedOffset);
    }
    if (overwriteValue)
      write(record.key(), oldValue, generationId, true);

    if (resignGenerationId != null)
      rebalanceListener.onWriterResigned(resignGenerationId);
  }

  /**
   * Callback invoked on the producer network thread when status record sent by
   * this writer has been written to the partition and acknowledged. If no rebalance has
   * occurred since this status was sent, update writer state. No other records are produced
   * by this writer until the status record has been consumed by the local reader.
   *
   * @param generationId Generation id of status record
   * @param status Status written
   * @param offset Offset of status record
   */
  private synchronized void onStatusRecordWriteCompletion(int generationId,
                                                          MetadataStoreStatus status,
                                                          long offset) {
    this.lastProducedOffset = offset;
    if (this.generationId == generationId) {
      log.debug("Status record of generation {} for partition {} written at offset {}",
          generationId, topicPartition, offset);
      if (lastConsumedOffset >= offset)
        status(status);
    } else {
      log.debug("Discarding status of generation {} for partition writer {} since generation has changed to {}",
          generationId, topicPartition, this.generationId);
    }
    notifyAll();
  }

  // Invoked on the producer network thread when send callback is processed
  // Reader could have already consumed this record on its thread. Ensure that pending writes are completed.

  /**
   * Callback invoked on the producer network thread when a record sent by this writer
   * has been written to the partition and acknowledged. If the reader has consumed this record,
   * notify the reader thread since it will be waiting until write completion. The pending write
   * is completed when the local reader has consumed this offset or beyond.
   *
   * @param pendingWrite Pending write instance corresponding to the callback
   * @param generationId Generation id of the writer at the time the record was sent
   * @param offset Offset to which the record was written
   */
  private synchronized void onRecordWriteCompletion(PendingWrite pendingWrite, int generationId, long offset) {
    log.debug("Send callback for record with partition {} generationId {} offset {}",
        topicPartition, generationId, offset);

    this.lastProducedOffset = offset;
    pendingWrite.offset = offset;
    maybeCompletePendingWrites(lastConsumedOffset);
  }

  /**
   * Fail a pending write with the provided exception.
   */
  private synchronized void onRecordWriteFailure(PendingWrite pendingWrite, Exception exception) {
    pendingWrite.fail(exception);
    pendingWrites.remove(pendingWrite);
  }

  /**
   * Sets status of this writer and notifies any waiting threads.
   * @param newStatus New status of this writer
   */
  private synchronized void status(MetadataStoreStatus newStatus) {
    if (newStatus != this.status) {
      log.debug("Changing status from {} to {}", this.status, newStatus);
      this.status = newStatus;
      if (status == MetadataStoreStatus.INITIALIZED)
        notifyAll();
    }
  }

  /**
   * Returns true if a pending write exists with the provided offset.
   * This is invoked by the current writer to check if a consumed record
   * was one that was written by this writer.
   */
  private boolean pendingWriteExists(long offset) {
    return pendingWrites.stream().anyMatch(p -> p.offset == offset);
  }

  private boolean waitUntilPendingOffsetsKnown(long consumedOffset) {
    return waitUntil(unused -> pendingWrites.isEmpty() || lastProducedOffset >= consumedOffset, false);
  }

  /**
   * Completes all pending writes up to and including the provided consumed offset.
   */
  private void maybeCompletePendingWrites(long consumedOffset) {
    Collection<PendingWrite> completedWrites = pendingWrites.stream()
        .filter(pendingWrite -> pendingWrite.maybeComplete(consumedOffset))
        .collect(Collectors.toList());
    pendingWrites.removeAll(completedWrites);
    notifyAll();
  }

  /**
   * Cancels any pending writes with lower generation than the provided generation id.
   */
  private void maybeCancelPendingWrites(int newGenerationId) {
    Collection<PendingWrite> cancelledWrites = pendingWrites.stream()
        .filter(pendingWrite -> pendingWrite.maybeCancel(newGenerationId))
        .collect(Collectors.toList());
    pendingWrites.removeAll(cancelledWrites);
    notifyAll();
  }

  private boolean waitUntil(Predicate<Boolean> predicate,
                            boolean failIfNotMaster) {
    try {
      int expectedGenerationId = generationId;
      if (generationId == NOT_MASTER_WRITER) {
        if (failIfNotMaster)
          throw notMasterWriterException();
        else
          return true;
      }

      long endMs = time.milliseconds() + refreshTimeout.toMillis();
      while (!predicate.test(null)) {
        long remainingMs = endMs - time.milliseconds();
        if (remainingMs <= 0)
          return false;
        wait(remainingMs);

        if (generationId != expectedGenerationId) {
          if (failIfNotMaster)
            throw notMasterWriterException();
          else
            return true;
        }
      }
      return true;
    } catch (InterruptedException e) {
      throw new InterruptException(e);
    }
  }

  private NotMasterWriterException notMasterWriterException() {
    return new NotMasterWriterException("This node is currently not the master writer for Metadata Service."
        + " This could be a transient exception during writer election.");
  }

  private class PendingWrite implements Callback {
    private final CompletableFuture<Void> future;
    private final int generationId;
    private final K key;
    private long offset;

    PendingWrite(int generationId, K key) {
      this.generationId = generationId;
      this.key = key;
      this.future = new CompletableFuture<>();
      this.offset = -1;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      log.debug("Pending write completed metadata={} exception={}", metadata, exception);
      if (!future.isDone()) {
        if (exception != null) {
          onRecordWriteFailure(this, exception);
        } else {
          onRecordWriteCompletion(this, generationId, metadata.offset());
        }
      }
    }

    void fail(Exception exception) {
      future.completeExceptionally(exception);
    }

    boolean maybeCancel(int newGenerationId) {
      if (this.generationId < newGenerationId) {
        log.debug("Cancelling pending write since rebalance occurred");
        NotMasterWriterException exception =
            new NotMasterWriterException("Update will be aborted since writer rebalance occurred");
        fail(exception);
        return true;
      } else
        return false;
    }

    boolean maybeComplete(long consumedOffset) {
      if (offset >= 0 && offset <= consumedOffset) {
        if (consumedOffset == offset)
          log.debug("Completing pending write since offset {} has been consumed", offset);
        else
          log.debug(
              "Completing pending write with offset {} since a higher offset {} has been consumed",
              offset, consumedOffset);
        future.complete(null);
        return true;
      } else
        return false;
    }

    @Override
    public String toString() {
      return "PendingWrite(" +
          "isDone=" + future.isDone() +
          ", generationId=" + generationId +
          ", key=" + key +
          ", offset=" + offset +
          ')';
    }
  }
}
