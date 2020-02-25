// (Copyright) [2018 - 2018] Confluent, Inc.
package io.confluent.kafka.multitenant.quota;

import io.confluent.kafka.multitenant.schema.TenantContext;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TestCluster {

  private final Map<TopicPartition, List<Integer>> topicPartitions = new HashMap<>();
  private final Map<Integer, Node> nodes = new HashMap<>();

  public void setPartitionLeaders(String topic, int firstPartition, int count,
                                  Integer leaderBrokerId) {
    for (int i = firstPartition; i < firstPartition + count; i++) {
      List<Integer> replicas = leaderBrokerId == null ? Collections.emptyList()
          : Collections.singletonList(leaderBrokerId);
      topicPartitions.put(new TopicPartition(topic, i), replicas);
    }
  }

  public void createPartitions(String topic, int firstPartition, List<List<Integer>> assignment) {
    for (int i = firstPartition; i < firstPartition + assignment.size(); i++) {
      topicPartitions.put(new TopicPartition(topic, i), assignment.get(i - firstPartition));
    }
  }

  public void deleteTopic(String topic) {
    topicPartitions.keySet().removeIf(tp -> tp.topic().equals(topic));
  }

  public void addNode(int id, String rack) {
    nodes.put(id, new Node(id, "host" + id, 9090, rack));
  }

  public boolean rackAware() {
    boolean rackAware = true;
    for (Node node : nodes.values()) {
      if (node.rack() == null) {
        rackAware = false;
        break;
      }
    }
    return rackAware;
  }

  public Set<String> racks() {
    Set<String> racks = new HashSet<>();
    for (Node node : nodes.values()) {
      if (node.rack() != null) {
        racks.add(node.rack());
      }
    }
    return racks;
  }

  public Map<Node, Integer> partitionCountByNode(String tenant, boolean leader) {
    Map<Node, Integer> result = new HashMap<>(nodes.size());
    for (Node node : nodes.values()) {
      result.put(node, 0);
    }

    String tenantPrefix = tenant + TenantContext.DELIMITER;
    for (Map.Entry<TopicPartition, List<Integer>> entry : topicPartitions.entrySet()) {
      if (!entry.getKey().topic().startsWith(tenantPrefix)) {
        continue;
      }
      List<Integer> replicas = entry.getValue();
      if (!replicas.isEmpty()) {
        if (leader) {
          Node leaderNode = nodes.get(replicas.get(0));
          result.computeIfPresent(leaderNode, (n, count) -> count + 1);
        } else {
          for (int i = 1; i < replicas.size(); i++) {
            Node node = nodes.get(replicas.get(i));
            result.computeIfPresent(node, (n, count) -> count + 1);
          }
        }
      }
    }
    return result;
  }

  public Cluster cluster() {
    Set<PartitionInfo> partitions = new HashSet<>();
    for (Map.Entry<TopicPartition, List<Integer>> entry : topicPartitions.entrySet()) {
      TopicPartition tp = entry.getKey();
      List<Integer> replicas = entry.getValue();
      Node leaderNode = replicas.isEmpty() ? null : nodes.get(replicas.get(0));
      Node[] replicaArr = new Node[replicas.size()];
      for (int i = 0; i < replicas.size(); i++) {
        replicaArr[i] = nodes.get(replicas.get(i));
      }
      partitions.add(new PartitionInfo(tp.topic(), tp.partition(), leaderNode,
              replicaArr, replicaArr, new Node[0]));
    }
    return new Cluster("cluster1",
        nodes.values(),
        partitions,
        Collections.emptySet(),
        Collections.emptySet());
  }
}