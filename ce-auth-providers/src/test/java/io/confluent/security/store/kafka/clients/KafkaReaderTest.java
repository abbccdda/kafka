// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.store.kafka.clients;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.security.store.KeyValueStore;
import io.confluent.security.store.MetadataStoreStatus;
import io.confluent.security.test.utils.RbacTestUtils;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class KafkaReaderTest {

  private final MockTime time = new MockTime();
  private final String topic = "testTopic";
  private Cluster cluster;
  private MockConsumer<String, String> consumer;
  private KafkaReader<String, String> reader;
  private Cache cache;
  private Listener listener;

  @Before
  public void setUp() throws Exception {
    cluster = RbacTestUtils.mockCluster(2);
    consumer = RbacTestUtils.mockConsumer(cluster, 1);
    cache = new Cache();
    listener = new Listener();
    this.reader = new KafkaReader<>(topic, 2, consumer, cache, listener, time);
  }

  @After
  public void tearDown() {
    if (reader != null)
      reader.close(Duration.ZERO);
  }

  @Test
  public void testReader() throws Exception {
    createTopic();
    startReader();

    verifyNewRecord(1, 1, "key1", "value1", null);
    assertEquals("value1", cache.get("key1"));

    verifyNewRecord(1, 2, "key2", "value2", null);
    assertEquals("value2", cache.get("key2"));

    verifyNewRecord(1, 3, "key1", "value3", "value1");
    assertEquals("value3", cache.get("key1"));

    verifyNewRecord(1, 4, "key1", null, "value3");
    assertNull(cache.get("key1"));

    verifyNewRecord(0, 1, "anotherkey1", "anothervalue1", null);
    assertEquals("anothervalue1", cache.get("anotherkey1"));
  }

  @Test
  public void testReaderPopulatesCacheBeforeCompletingFuture() throws Exception {
    createTopic();

    Map<TopicPartition, Long> offsets = new HashMap<>();
    offsets.put(new TopicPartition(topic, 0), 0L);
    offsets.put(new TopicPartition(topic, 1), 0L);
    consumer.updateBeginningOffsets(offsets);

    offsets.put(new TopicPartition(topic, 0), 5L);
    offsets.put(new TopicPartition(topic, 1), 10L);
    consumer.updateEndOffsets(offsets);

    CompletableFuture<Void> future = startReader();
    assertFalse(future.isDone());

    verifyNewRecord(0, 0, "STATUS-0", "INITIALIZED", null);
    verifyNewRecord(1, 0, "STATUS-1", "INITIALIZED", null);

    for (int i = 1; i < 5; i++) {
      verifyNewRecord(0, i, "key", "value", cache.get("key"));
      verifyNewRecord(1, i, "key", "value", cache.get("key"));
    }
    assertFalse(future.isDone());
    for (int i = 5; i < 10; i++) {
      verifyNewRecord(0, i, "key", "value", "value");
    }
    assertFalse(future.isDone());

    for (int i = 5; i < 10; i++) {
      verifyNewRecord(1, i, "key", "value", "value");
    }
    future.get(5, TimeUnit.SECONDS);
    assertTrue(future.isDone());
    assertEquals("value", cache.get("key"));
  }

  @Test
  public void testReaderWaitsForInitializationBeforeCompletingFuture() throws Exception {
    createTopic();

    Map<TopicPartition, Long> offsets = new HashMap<>();
    offsets.put(new TopicPartition(topic, 0), 0L);
    offsets.put(new TopicPartition(topic, 1), 0L);
    consumer.updateBeginningOffsets(offsets);

    offsets.put(new TopicPartition(topic, 0), 5L);
    offsets.put(new TopicPartition(topic, 1), 5L);
    consumer.updateEndOffsets(offsets);

    CompletableFuture<Void> future = startReader();
    assertFalse(future.isDone());

    for (int i = 1; i < 5; i++) {
      verifyNewRecord(0, i, "key", "value", cache.get("key"));
      verifyNewRecord(1, i, "key", "value", cache.get("key"));
    }
    assertFalse(future.isDone());

    verifyNewRecord(0, 6, "STATUS-0", "INITIALIZED", null);
    assertFalse(future.isDone());
    verifyNewRecord(1, 7, "STATUS-1", "INITIALIZED", null);

    future.get(5, TimeUnit.SECONDS);
    assertTrue(future.isDone());
    assertEquals("value", cache.get("key"));
  }

  @Test
  public void testReaderDoesNotWaitForNewWriterBeforeCompletingFuture() throws Exception {
    createTopic();

    Map<TopicPartition, Long> offsets = new HashMap<>();
    offsets.put(new TopicPartition(topic, 0), 0L);
    offsets.put(new TopicPartition(topic, 1), 0L);
    consumer.updateBeginningOffsets(offsets);

    offsets.put(new TopicPartition(topic, 0), 5L);
    offsets.put(new TopicPartition(topic, 1), 5L);
    consumer.updateEndOffsets(offsets);

    consumer.assign(Arrays.asList(new TopicPartition(topic, 0), new TopicPartition(topic, 1)));

    for (int partition = 0; partition < 2; partition++) {
      for (int i = 1; i < 5; i++) {
        consumer.addRecord(new ConsumerRecord<>(topic, partition, i, "key", "value"));
      }
      consumer.addRecord(new ConsumerRecord<>(topic, partition, 6, "STATUS-" + partition, "INITIALIZED"));
      consumer.addRecord(new ConsumerRecord<>(topic, partition, 7, "STATUS-" + partition, "FAILED"));
      consumer.addRecord(new ConsumerRecord<>(topic, partition, 8, "STATUS-" + partition, "INITIALIZING"));
    }
    consumer.unsubscribe();

    listener.verifyRecords = false;
    CompletableFuture<Void> future = startReader();
    future.get(5, TimeUnit.SECONDS);
    assertTrue(future.isDone());
    assertEquals("value", cache.get("key"));
  }

  @Test
  public void testStatus() throws Exception {
    createTopic();
    startReader();

    verifyNewRecord(1, 1, "STATUS-1", MetadataStoreStatus.INITIALIZING.name(), null);
    assertEquals(MetadataStoreStatus.INITIALIZING, cache.status(1));

    verifyNewRecord(0, 1, "STATUS-0", MetadataStoreStatus.INITIALIZED.name(), null);
    assertEquals(MetadataStoreStatus.INITIALIZED, cache.status(0));

    cache.fail(1, "Failed");
    assertEquals(MetadataStoreStatus.FAILED, cache.status(1));
  }

  @Test
  public void testTopicCreateTimeout() throws Exception {
    CompletableFuture<Void> future = reader.start(Duration.ofSeconds(100))
        .toCompletableFuture();
    time.sleep(100 * 1000);
    Throwable e = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
    assertEquals(TimeoutException.class, e.getCause().getClass());
  }

  @Test
  public void testMissingPartitionMetadata() throws Exception {
    Node[] replicas = cluster.nodes().toArray(new Node[cluster.nodes().size()]);
    PartitionInfo tp = new PartitionInfo(topic, 1, cluster.nodeById(0), replicas, replicas);
    consumer.updatePartitions(topic, Collections.singletonList(tp));
    CompletableFuture<Void> future = reader.start(Duration.ofSeconds(100))
        .toCompletableFuture();
    time.sleep(100 * 1000);
    Throwable e = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
    assertEquals(TimeoutException.class, e.getCause().getClass());
  }

  @Test
  public void testUnexpectedPartition() throws Exception {
    Node[] replicas = cluster.nodes().toArray(new Node[cluster.nodes().size()]);
    PartitionInfo tp = new PartitionInfo(topic, 2, cluster.nodeById(0), replicas, replicas);
    consumer.updatePartitions(topic, Collections.singletonList(tp));
    CompletableFuture<Void> future = reader.start(Duration.ofSeconds(100)).toCompletableFuture();
    // Should fail before timeout
    Throwable e = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertEquals(IllegalStateException.class, e.getCause().getClass());
  }

  @Test
  public void testTooManyPartitions() throws Exception {
    Node[] replicas = cluster.nodes().toArray(new Node[cluster.nodes().size()]);
    List<PartitionInfo> partitions = IntStream.range(0, 3)
        .mapToObj(i -> new PartitionInfo(topic, i, cluster.nodeById(0), replicas, replicas))
        .collect(Collectors.toList());
    consumer.updatePartitions(topic, partitions);
    CompletableFuture<Void> future = reader.start(Duration.ofSeconds(100)).toCompletableFuture();
    // Should fail before timeout
    Throwable e = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertEquals(IllegalStateException.class, e.getCause().getClass());
  }

  private CompletableFuture<Void> startReader() throws Exception {
    updatePartitions(topic);
    CompletableFuture<Void> future = reader.start(Duration.ofMillis(100))
        .toCompletableFuture();
    TestUtils.waitForCondition(() -> reader.numPartitions() > 0, "Reader not initialized");
    return future;
  }

  private void updatePartitions(String topic) {
    Node[] replicas = cluster.nodes().toArray(new Node[cluster.nodes().size()]);
    PartitionInfo tp1 = new PartitionInfo(topic, 0, cluster.nodeById(0), replicas, replicas);
    PartitionInfo tp2 = new PartitionInfo(topic, 1, cluster.nodeById(0), replicas, replicas);
    consumer.updatePartitions(topic, Arrays.asList(tp1, tp2));
  }

  private void createTopic() {
    Node node = cluster.nodes().get(0);
    PartitionInfo p0 = new PartitionInfo(topic, 0, node, new Node[] {node}, new Node[] {node});
    PartitionInfo p1 = new PartitionInfo(topic, 1, node, new Node[] {node}, new Node[] {node});
    consumer.updatePartitions(topic, Arrays.asList(p0, p1));
    Map<TopicPartition, Long> offsets = new HashMap<>();
    offsets.put(new TopicPartition(topic, 0), 0L);
    offsets.put(new TopicPartition(topic, 1), 0L);
    consumer.updateBeginningOffsets(offsets);
    consumer.updateEndOffsets(offsets);
  }

  private void verifyNewRecord(int partition, long offset, String key, String newValue, String oldValue) throws Exception {
    ConsumerRecord<String, String> nextRecord = new ConsumerRecord<>(topic, partition, offset, key, newValue);
    listener.expectedNewRecord = nextRecord;
    listener.expectedOldValue = oldValue;
    consumer.addRecord(nextRecord);
    TestUtils.waitForCondition(() -> listener.consumedOffsets.getOrDefault(partition, -1L) == offset,
        "Record not consumed");
  }

  private static class Cache implements KeyValueStore<String, String> {

    private final Map<String, String> map = new HashMap<>();

    @Override
    public String get(String key) {
      return map.get(key);
    }

    @Override
    public String put(String key, String value) {
      return map.put(key, value);
    }

    @Override
    public String remove(String key) {
      return map.remove(key);
    }

    @Override
    public Map<? extends String, ? extends String> map(String entryType) {
      return map;
    }

    @Override
    public void fail(int partition, String errorMessage) {
      put("STATUS-" + partition, MetadataStoreStatus.FAILED.toString());
    }

    @Override
    public MetadataStoreStatus status(int partition) {
      String status = get("STATUS-" + partition);
      return status == null ? MetadataStoreStatus.UNKNOWN : MetadataStoreStatus.valueOf(status);
    }
  }

  private class Listener implements ConsumerListener<String, String> {
    private Map<Integer, Long> consumedOffsets = new HashMap<>(2);
    private ConsumerRecord<String, String> expectedNewRecord;
    private String expectedOldValue;
    private volatile boolean verifyRecords = true;

    @Override
    public void onConsumerRecord(ConsumerRecord<String, String> record, String oldValue) {
      if (verifyRecords) {
        assertEquals(expectedNewRecord, record);
        assertEquals(expectedOldValue, oldValue);
      }
      consumedOffsets.put(record.partition(), record.offset());
    }
  }
}
