// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.test.utils;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import io.confluent.security.auth.store.cache.DefaultAuthCache;
import io.confluent.security.auth.store.data.RoleBindingKey;
import io.confluent.security.auth.store.data.RoleBindingValue;
import io.confluent.security.auth.store.data.UserKey;
import io.confluent.security.auth.store.data.UserValue;
import io.confluent.security.auth.store.kafka.KafkaAuthStore;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;

public class RbacTestUtils {

  public static void updateRoleBinding(DefaultAuthCache authCache,
                                       KafkaPrincipal principal,
                                       String role,
                                       Scope scope,
                                       Set<ResourcePattern> resources) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    RoleBindingValue value = new RoleBindingValue(resources == null ? Collections.emptySet() : resources);
    authCache.put(key, value);
  }

  public static void deleteRoleBinding(DefaultAuthCache authCache,
                                       KafkaPrincipal principal,
                                       String role,
                                       Scope scope) {
    RoleBindingKey key = new RoleBindingKey(principal, role, scope);
    authCache.remove(key);
  }

  public static void updateUser(DefaultAuthCache authCache, KafkaPrincipal user, Collection<KafkaPrincipal> groups) {
    authCache.put(new UserKey(user), new UserValue(groups));
  }

  public static void deleteUser(DefaultAuthCache authCache, KafkaPrincipal user) {
    authCache.remove(new UserKey(user));
  }

  public static Cluster mockCluster(int numNodes) {
    Node[] nodes = new Node[numNodes];
    for (int i = 0; i < numNodes; i++)
      nodes[i] = new Node(i, "host" + i, 9092);
    Node[] replicas = numNodes == 1 ? new Node[]{nodes[0]} : new Node[]{nodes[0], nodes[1]};
    PartitionInfo partitionInfo = new PartitionInfo(KafkaAuthStore.AUTH_TOPIC, 0,
        nodes[0], replicas, replicas);
    return new Cluster("cluster", Utils.mkSet(nodes),
        Collections.singleton(partitionInfo), Collections.emptySet(), Collections.emptySet());
  }

  public static <K, V> MockConsumer<K, V> mockConsumer(Cluster cluster, int numAuthTopicPartitions) {
    Node node = cluster.nodes().get(0);
    List<PartitionInfo> partitionInfos = new ArrayList<>(numAuthTopicPartitions);
    Map<TopicPartition, Long> offsets = new HashMap<>(numAuthTopicPartitions);

    for (int i = 0; i < numAuthTopicPartitions; i++) {
      partitionInfos.add(new PartitionInfo(KafkaAuthStore.AUTH_TOPIC,
          i,
          node,
          new Node[]{node},
          new Node[]{node}));
      offsets.put(new TopicPartition(KafkaAuthStore.AUTH_TOPIC, i), 0L);
    }

    MockConsumer<K, V> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.updatePartitions(KafkaAuthStore.AUTH_TOPIC, partitionInfos);
    consumer.updateBeginningOffsets(offsets);
    consumer.updateEndOffsets(offsets);
    return consumer;
  }

  public static void verifyMetric(String name, String type, long min, long max) {
    Set<Metric> metrics = metrics(name, type);
    assertFalse(metrics.isEmpty());
    long value = metrics.stream().mapToLong(RbacTestUtils::metricValue).sum();
    assertTrue("Unexpected value: " + value, value >= min && value <= max);
  }

  private static Set<Metric> metrics(String name, String type) {
    return Metrics.defaultRegistry().allMetrics().entrySet().stream()
        .filter(e -> e.getKey().getName().equals(name) && e.getKey().getType().equals(type))
        .map(Map.Entry::getValue)
        .collect(Collectors.toSet());
  }

  @SuppressWarnings("unchecked")
  private static long metricValue(Metric metric) {
    if (metric instanceof Gauge) {
      return ((Gauge<Number>) metric).value().longValue();
    } else if (metric instanceof Meter) {
      return ((Meter) metric).count();
    } else
      throw new IllegalArgumentException("Metric value could not be computed for " + metric);
  }

  public static void verifyMetadataStoreMetrics() throws Exception {
    RbacTestUtils.verifyMetric("reader-failure-start-seconds-ago", "KafkaAuthStore", 0, 0);
    RbacTestUtils.verifyMetric("remote-failure-start-seconds-ago", "KafkaAuthStore", 0, 0);
    RbacTestUtils.verifyMetric("writer-failure-start-seconds-ago", "KafkaAuthStore", 0, 0);
    RbacTestUtils.verifyMetric("active-writer-count", "KafkaAuthStore", 0, 1);
    TestUtils.waitForCondition(() -> {
      Set<Metric> metrics = metrics("active-writer-count", "KafkaAuthStore");
      return metrics.stream().mapToLong(RbacTestUtils::metricValue).sum() == 1;
    }, "Writer not elected within timeout");
  }
}
