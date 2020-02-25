// (Copyright) [2018 - 2018] Confluent, Inc.
package io.confluent.kafka.multitenant.quota;

import io.confluent.kafka.multitenant.quota.ClusterMetadata.ReplicaCounts;
import io.confluent.kafka.multitenant.quota.TenantPartitionAssignor.TopicInfo;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TenantPartitionAssignorTest {

  private final TestCluster testCluster = new TestCluster();
  private TenantPartitionAssignor partitionAssignor;


  @Before
  public void setUp() {
    partitionAssignor = new TenantPartitionAssignor();
    partitionAssignor.updateClusterMetadata(testCluster.cluster());
  }

  /**
   * Basic test of rack-unaware assignment. This is the example from the javadoc.
   * Verify that we generate similar assignment if there are no other existing topics.
   */
  @Test
  public void testRackUnawareAssignment() {
    String topic = "tenant1_topicA";
    addNodes(5, 0);
    createTopic(topic, 10, 3);
    List<List<Integer>> expectedAssignment = Arrays.asList(
        Arrays.asList(0, 1, 2),
        Arrays.asList(1, 2, 3),
        Arrays.asList(2, 3, 4),
        Arrays.asList(3, 4, 0),
        Arrays.asList(4, 0, 1),
        Arrays.asList(0, 2, 3),
        Arrays.asList(1, 3, 4),
        Arrays.asList(2, 4, 0),
        Arrays.asList(3, 0, 1),
        Arrays.asList(4, 1, 2)
    );
    verifyAssignment(expectedAssignment, testCluster.cluster().partitionsForTopic(topic));
    verifyAssignmentsAreBalanced(0, 0, 0);
  }

  @Test
  public void testRackUnawareAssignmentOneNodeCluster() {
    String topic = "tenant1_topicA";
    addNodes(1, 0);
    createTopic(topic, 5, 1);
    List<List<Integer>> expectedAssignment = Arrays.asList(
        Collections.singletonList(0),
        Collections.singletonList(0),
        Collections.singletonList(0),
        Collections.singletonList(0),
        Collections.singletonList(0)
    );
    verifyAssignment(expectedAssignment, testCluster.cluster().partitionsForTopic(topic));
  }

  /**
   * Basic test of rack-aware assignment. This is the example from the javadoc.
   * Verify that we generate similar assignment if there are no other existing topics.
   */
  @Test
  public void testRackAwareAssignment() {
    String topic = "tenant1_topicA";
    testCluster.addNode(0, "rack1");
    testCluster.addNode(1, "rack3");
    testCluster.addNode(2, "rack3");
    testCluster.addNode(3, "rack2");
    testCluster.addNode(4, "rack2");
    testCluster.addNode(5, "rack1");
    partitionAssignor.updateClusterMetadata(testCluster.cluster());

    createTopic(topic, 12, 3);
    // Expected assignment with rack alternate list 0, 1, 3, 5, 2, 4
    List<List<Integer>> expectedAssignment = Arrays.asList(
        Arrays.asList(0, 1, 3), // shift = 1
        Arrays.asList(1, 3, 5),
        Arrays.asList(3, 5, 2),
        Arrays.asList(5, 2, 4),
        Arrays.asList(2, 4, 0),
        Arrays.asList(4, 0, 1),
        Arrays.asList(0, 2, 4), // shift = 4
        Arrays.asList(1, 4, 0),
        Arrays.asList(3, 0, 1),
        Arrays.asList(5, 1, 3),
        Arrays.asList(2, 3, 5),
        Arrays.asList(4, 5, 2)
    );
    verifyAssignment(expectedAssignment, testCluster.cluster().partitionsForTopic(topic));
    verifyAssignmentsAreBalanced(0, 0, 0);
  }

  /**
   * Tests that nodes are ordered correctly by the partition assignor.
   * Order of precedence is:
   *   1. Existing partition replica count (used for partition add)
   *   2. Tenant partition count
   *   3. Total partition count
   */
  @Test
  public void testNodeOrderingByPartitionCount() {
    addNodes(3, 0);
    testCluster.setPartitionLeaders("tenant1_topicA", 0, 10, 1);
    testCluster.setPartitionLeaders("tenant1_topicB", 0, 2, 2);
    testCluster.setPartitionLeaders("tenant2_topicA", 0, 5, 2);
    testCluster.setPartitionLeaders("tenant2_topicB", 0, 2, 1);

    verifyTopicCreateNodeOrder("tenant1", 0, 2, 1);
    verifyTopicCreateNodeOrder("tenant2", 0, 1, 2);
    verifyPartitionAddNodeOrder("tenant1", "tenant1_topicA", 0, 2, 1);
    verifyPartitionAddNodeOrder("tenant1", "tenant1_topicB", 0, 1, 2);
    verifyPartitionAddNodeOrder("tenant2", "tenant2_topicA", 0, 1, 2);
    verifyPartitionAddNodeOrder("tenant2", "tenant2_topicB", 0, 2, 1);

    testCluster.addNode(3, null);
    testCluster.setPartitionLeaders("tenant1_topicC", 0, 7, 3);

    verifyTopicCreateNodeOrder("tenant1", 0, 2, 3, 1);
    verifyPartitionAddNodeOrder("tenant1", "tenant1_topicB", 0, 3, 1, 2);
  }

  private void verifyTopicCreateNodeOrder(String tenant, Integer... expectedNodeIds) {
    partitionAssignor.updateClusterMetadata(testCluster.cluster());
    ClusterMetadata clusterMetadata = new ClusterMetadata(tenant, testCluster.cluster());
    Map<Integer, ReplicaCounts> replicaCounts = clusterMetadata.nodeReplicaCounts(Collections.emptyList(), true);
    List<Integer> nodes = TenantPartitionAssignor.orderNodes(replicaCounts);
    assertEquals(Arrays.asList(expectedNodeIds), nodes);
  }

  private void verifyPartitionAddNodeOrder(String tenant, String topic,
                                           Integer... expectedNodeIds) {
    partitionAssignor.updateClusterMetadata(testCluster.cluster());
    ClusterMetadata clusterMetadata = new ClusterMetadata(tenant, testCluster.cluster());
    Map<Integer, ReplicaCounts> replicaCounts = clusterMetadata.nodeReplicaCounts(
        testCluster.cluster().partitionsForTopic(topic), true);
    List<Integer> nodes = TenantPartitionAssignor.orderNodes(replicaCounts);
    assertEquals(Arrays.asList(expectedNodeIds), nodes);
  }

  /**
   * Test that rack-unaware topic creation with partition counts that are multiples of
   * broker count result in perfectly balanced assignment for leaders and followers.
   */
  @Test
  public void testRackUnawareCreateTopicsBrokerMultiples() {
    verifyCreateTopicsBrokerMultiples(0);
  }

  /**
   * Test that rack-aware topic creation with partition counts that are multiples of
   * broker count result in perfectly balanced assignment for leaders and followers.
   */
  @Test
  public void testRackAwareCreateTopicsBrokerMultiples() {
    verifyCreateTopicsBrokerMultiples(3);
  }

  private void verifyCreateTopicsBrokerMultiples(int racks) {
    int numBrokers = 6;
    addNodes(numBrokers, racks);
    for (int i = 1; i <= 5; i++) {
      createTopic("tenant1_topicA" + i, numBrokers, 3);
      verifyAssignmentsAreBalanced(0, 0, 0);
    }
    for (int i = 1; i <= 5; i++) {
      createTopic("tenant2_topicB" + i, numBrokers, 3);
      verifyAssignmentsAreBalanced(0, 0, 0);
    }

    for (int i = 1; i <= 10; i++) {
      createTopic("tenant1_topicC" + i, numBrokers * i, 3);
      verifyAssignmentsAreBalanced(0, 0, 0);
    }
    for (int i = 1; i <= 10; i++) {
      createTopic("tenant2_topicD" + i, numBrokers * i, 3);
      verifyAssignmentsAreBalanced(0, 0, 0);
    }
  }

  /**
   * Test that rack-unaware partition addition with partition counts that are multiples of
   * broker count result in perfectly balanced assignment for leaders and followers.
   */
  @Test
  public void testRackUnawareAddPartitionsBrokerMultiples() {
    verifyAddPartitionsBrokerMultiples(0);
  }

  /**
   * Test that rack-unaware partition addition with partition counts that are multiples of
   * broker count result in perfectly balanced assignment for leaders and followers.
   */
  @Test
  public void testRackAwareAddPartitionsBrokerMultiples() {
    verifyAddPartitionsBrokerMultiples(3);
  }

  private void verifyAddPartitionsBrokerMultiples(int racks) {
    int numBrokers = 6;
    addNodes(numBrokers, racks);
    createTopic("tenant1_topicA", numBrokers, 3);
    for (int i = 1; i <= 5; i++) {
      addPartitions("tenant1_topicA", numBrokers * i, numBrokers);
      verifyAssignmentsAreBalanced(0, 0, 0);
    }
    createTopic("tenant2_topicB", numBrokers, 3);
    for (int i = 1; i <= 5; i++) {
      addPartitions("tenant2_topicB", numBrokers * i, numBrokers);
      verifyAssignmentsAreBalanced(0, 0, 0);
    }

    createTopic("tenant1_topicC", numBrokers, 3);
    for (int i = 1, p = numBrokers; i <= 10; p += numBrokers * i++) {
      addPartitions("tenant1_topicC", p, numBrokers * i);
      verifyAssignmentsAreBalanced(0, 0, 0);
    }
    createTopic("tenant2_topicD", numBrokers, 3);
    for (int i = 1, p = numBrokers; i <= 10; p += numBrokers * i++) {
      addPartitions("tenant2_topicD", p, numBrokers * i);
      verifyAssignmentsAreBalanced(0, 0, 0);
    }
  }

  /**
   * Simple assignment test for rack-unaware topic creation that verifies that
   * leaders are balanced and follower imbalance is less than replication factor.
   */
  @Test
  public void testRackUnawareTopicCreate() {
    addNodes(3, 0);

    createTopic("tenant1_topicA", 3, 2);
    verifyPartitionCountsOnNodes(true, 1, 1, 1);
    verifyPartitionCountsOnNodes(false, 1, 1, 1);

    createTopic("tenant2_topicA", 5, 2);
    verifyAssignments();
    verifyPartitionCountsOnNodes(true, 2, 3, 3);
    verifyAssignmentsAreBalanced("tenant2", 2);

    createTopic("tenant2_topicB", 4, 2);
    verifyAssignments();
    verifyPartitionCountsOnNodes(true, 4, 4, 4);
    verifyAssignmentsAreBalanced("tenant2", 2);
  }

  /**
   * Simple assignment test for rack-aware topic creaton that verifies
   * that leaders are balanced and follower imbalance is limited by
   * (replicationFactor - 1) * numRacks.
   */
  @Test
  public void testRackAwareTopicCreate() {
    addNodes(6, 3);

    createTopic("tenant1_topicA", 6, 3);
    verifyAssignments();
    verifyPartitionCountsOnNodes(true, 1, 1, 1, 1, 1, 1);
    verifyPartitionCountsOnNodes(false, 2, 2, 2, 2, 2, 2);

    createTopic("tenant2_topicA", 9, 3);
    verifyAssignments();
    verifyPartitionCountsOnNodes(true, 2, 2, 2, 3, 3, 3);
    verifyPartitionCountsOnNodes(false, 5, 5, 5, 5, 5, 5);
    verifyAssignmentsAreBalanced(1, 1, 1);

    createTopic("tenant2_topicB", 3, 3);
    verifyAssignments();
    verifyPartitionCountsOnNodes(false, 6, 6, 6, 6, 6, 6);
    verifyAssignmentsAreBalanced(0, 2, 2);
  }

  /**
   * Simple assignment test for rack-unaware partition addition that verifies that
   * leaders are balanced and follower imbalance is less than replication factor.
   */
  @Test
  public void testRackUnawarePartitionAdd() {
    addNodes(3, 0);

    createTopic("tenant1_topicA", 5, 2);

    addPartitions("tenant1_topicA", 5, 4);
    verifyAssignments();
    verifyPartitionCountsOnNodes(true, 3, 3, 3);
    verifyAssignmentsAreBalanced("tenant1", 2);
  }

  /**
   * Simple assignment test for rack-aware partition addition that verifies
   * that leaders are balanced and follower imbalance is limited by
   * (replicationFactor - 1) * numRacks.
   */
  @Test
  public void testRackAwarePartitionAdd() {
    addNodes(6, 3);
    createTopic("tenant1_topicA", 9, 3);

    addPartitions("tenant1_topicA", 9, 3);
    verifyAssignments();
    verifyPartitionCountsOnNodes(true, 2, 2, 2, 2, 2, 2);
    verifyAssignmentsAreBalanced(0, 2, 2);
  }

  /**
   * Tests that rack-unaware assignments vary for topics even when
   * partitions of a cluster are balanced
   */
  @Test
  public void testRackUnawareAssignmentVariation() {
    addNodes(5, 0);

    createTopic("tenant1_topicA", 5, 3);
    verifyPartitionCountsOnNodes(true, 1, 1, 1, 1, 1);
    verifyPartitionCountsOnNodes(false, 2, 2, 2, 2, 2);

    createTopic("tenant1_topicB", 10, 3);
    verifyPartitionCountsOnNodes(true, 3, 3, 3, 3, 3);
    verifyPartitionCountsOnNodes(false, 6, 6, 6, 6, 6);

    verifyAssignmentVariation();
  }

  /**
   * Tests that rack-aware assignments vary for topics even when
   * partitions of a cluster are balanced
   */
  @Test
  public void testRackAwareAssignmentVariation() {
    addNodes(6, 3);

    createTopic("tenant1_topicA", 6, 3);
    verifyPartitionCountsOnNodes(true, 1, 1, 1, 1, 1, 1);
    verifyPartitionCountsOnNodes(false, 2, 2, 2, 2, 2, 2);

    createTopic("tenant1_topicB", 12, 3);
    verifyPartitionCountsOnNodes(true, 3, 3, 3, 3, 3, 3);
    verifyPartitionCountsOnNodes(false, 6, 6, 6, 6, 6, 6);

    verifyAssignmentVariation();
  }

  private void verifyAssignmentVariation() {
    Cluster cluster = testCluster.cluster();
    Set<List<Integer>> allReplicas = new HashSet<>();
    int numPartitions = 0;
    for (String topic : cluster.topics()) {
      for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
        List<Integer> partitionReplicas = new ArrayList<>();
        for (Node replica : partitionInfo.replicas()) {
          partitionReplicas.add(replica.id());
        }
        allReplicas.add(partitionReplicas);
        numPartitions++;
      }
    }
    assertTrue("Too few replica combinations " + allReplicas.size() + " for " + numPartitions,
            allReplicas.size() >= 0.5 * numPartitions);
  }

  /**
   * Tests that partition assignment works even if replicas of some
   * partitions of the tenant are on brokers that are not currently available.
   */
  @Test
  public void testUnavailableBrokers() {
    addNodes(6, 3);
    for (int i = 0; i < 5; i++) {
      testCluster.setPartitionLeaders("tenant1_topicA", 1, 5, 10 + i);
    }
    assertEquals(6, testCluster.cluster().nodes().size());

    createTopic("tenant1_topicB", 6, 3);
    verifyPartitionCountsOnNodes(true, 1, 1, 1, 1, 1, 1);
    verifyPartitionCountsOnNodes(false, 2, 2, 2, 2, 2, 2);

  }

  /**
   * Verifies that assignment of multiple new topics in a sequence and
   * the assignment of partition addition to the topics meet the replica
   * balance criteria for rack-unware assignment. Leaders must be balanced
   * and follower imbalance must be limited by maximum replicationFactor-1.
   */
  @Test
  public void testRackUnawareAssignmentsAreBalanced() {
    addNodes(3, 0);
    verifyTopicCreation("tenant1");
    verifyPartitionAddition("tenant1");
  }

  /**
   * Verifies that assignment of multiple new topics in a sequence and
   * the assignment of partition addition to the topics meet the replica
   * balance criteria for rack-aware assignment. Leaders must be balanced
   * and follower imbalance must be limited by (replicationFactor-1) * numRacks
   */
  @Test
  public void testRackAwareAssignmentsAreBalanced() {
    addNodes(6, 3);
    verifyTopicCreation("tenant1");
    verifyPartitionAddition("tenant1");
  }

  private void verifyTopicCreation(String tenant) {
    // Create some partitions for other tenants, these should not impact assignment
    testCluster.setPartitionLeaders("otherTenant_topicA", 0, 5, 2);
    testCluster.setPartitionLeaders("otherTenant_topicB", 0, 2, 1);

    // Create one partition and verify tenant's partitions are balanced
    TopicInfo topicInfo = new TopicInfo(10, (short) 3, 0);
    Map<String, TopicInfo> newTopics = Collections.singletonMap(tenant + "_topicA", topicInfo);
    Map<String, List<List<Integer>>> assignment = assignNewTopics(tenant, newTopics);
    verifyAssignmentsAreBalanced(tenant, assignment, 3);

    // Create another partition and  verify tenant's partitions are still balanced
    topicInfo = new TopicInfo(10, (short) 2, 0);
    newTopics = Collections.singletonMap(tenant + "_topicB", topicInfo);
    assignment = assignNewTopics(tenant, newTopics);
    verifyAssignmentsAreBalanced("tenant1", assignment, 3);

    // Create multiple topics in one request and verify tenant's partitions are balanced
    newTopics = new HashMap<>();
    newTopics.put(tenant + "_topicC", new TopicInfo(10, (short) 1, 0));
    newTopics.put(tenant + "_topicD", new TopicInfo(8, (short) 2, 0));
    newTopics.put(tenant + "_topicE", new TopicInfo(7, (short) 3, 0));
    assignment = assignNewTopics(tenant, newTopics);
    verifyAssignmentsAreBalanced(tenant, assignment, 3);
  }

  private void verifyPartitionAddition(String tenant) {
    Map<String, Integer> newPartitions = new HashMap<>();
    int partitionsToAdd = 2;
    int maxReplicationFactor = 0;
    for (String topic : testCluster.cluster().topics()) {
      if (topic.startsWith(tenant)) {
        List<PartitionInfo> partitionInfos = testCluster.cluster().partitionsForTopic(topic);
        int firstNewPartition = partitionInfos.size();
        newPartitions.put(topic, firstNewPartition + partitionsToAdd);
        partitionsToAdd++;
        int replicationFactor = partitionInfos.get(0).replicas().length;
        if (replicationFactor > maxReplicationFactor) {
          maxReplicationFactor = replicationFactor;
        }
      }
    }
    assertFalse("No tenant topics", newPartitions.isEmpty());
    Map<String, List<List<Integer>>> assignment = assignNewPartitions(newPartitions);
    verifyAssignmentsAreBalanced(tenant, assignment, maxReplicationFactor);
  }

  /**
   * Verifies that sequences of arbitrary topic creation and deletion
   * leads to assignments that are as balanced as possible for rack-unaware
   * assignment. Slots freed by delete must be allocated on subsequent
   * creates to keep the replicas of tenants balanced.
   */
  @Test
  public void testRackUnawareTopicCreateDelete() {
    addNodes(6, 0);
    verifyTopicCreateDelete();
  }

  /**
   * Verifies that sequences of arbitrary topic creation and deletion
   * leads to assignments that are as balanced as possible for rack-aware
   * assignment. Slots freed by delete must be allocated on subsequent
   * creates to keep the replicas of tenants balanced.
   */
  @Test
  public void testRackAwareTopicCreateDelete() {
    addNodes(6, 3);
    verifyTopicCreateDelete();
  }

  private void verifyTopicCreateDelete() {
    Random random = new Random();
    List<String> tenants = Arrays.asList("tenant1", "tenant2", "tenant3");
    List<String> topics = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String tenant = tenants.get(random.nextInt(tenants.size()));
      if (i > 5 && (i % 5) == 0) {
        testCluster.deleteTopic(topics.remove(random.nextInt(topics.size())));
      } else {
        String topic = tenant + "_" + "Topic" + i;
        int partitions = random.nextInt(20);
        createTopic(topic, partitions == 0 ? 1 : partitions, 3);
        topics.add(topic);
      }
    }
    for (String tenant : tenants) {
      // Since topics have been randomly deleted, partitions may be slightly
      // imbalanced. Create some additional topics and retry. They should
      // eventually become balanced.
      int maxRetries = 100;
      for (int i = 0; i < maxRetries; i++) {
        try {
          verifyAssignmentsAreBalanced(tenant, 3);
          break;
        } catch (Throwable t) {
          if (i == maxRetries - 1) {
            throw t;
          } else {
            createTopic(tenant + "_" + "TopicA" + i, 2, 3);
          }
        }
      }
    }
  }

  @Test
  public void testRackAwareOnlyIfAllBrokersHaveRack() {
    TestCluster cluster1 = new TestCluster();
    cluster1.addNode(1, null);
    assertFalse(new ClusterMetadata("tenant", cluster1.cluster()).rackAware());
    cluster1.addNode(2, null);
    assertFalse(new ClusterMetadata("tenant", cluster1.cluster()).rackAware());
    cluster1.addNode(3, "rack1");
    assertFalse(new ClusterMetadata("tenant", cluster1.cluster()).rackAware());

    TestCluster cluster2 = new TestCluster();
    cluster2.addNode(1, "rack1");
    assertTrue(new ClusterMetadata("tenant", cluster2.cluster()).rackAware());
    testCluster.addNode(2, "rack2");
    assertTrue(new ClusterMetadata("tenant", cluster2.cluster()).rackAware());
    cluster2.addNode(3, null);
    assertFalse(new ClusterMetadata("tenant", cluster2.cluster()).rackAware());
  }

  @Test
  public void testRackAlternatedBrokerList() {
    int numRacks = 3;
    int numBrokers = numRacks * 4;
    Map<Integer, String> brokerRacks = new HashMap<>();
    List<Integer> brokerList = new ArrayList<>();
    for (int i = 0; i < numBrokers; i++) {
      brokerRacks.put(i, "rack" + (i % numRacks));
      brokerList.add(i);
    }
    assertEquals(brokerList,
        TenantPartitionAssignor.rackAlternatedBrokerList(brokerList, brokerRacks, Collections.emptyList()));
    assertEquals(brokerList,
        TenantPartitionAssignor.rackAlternatedBrokerList(Arrays.asList(0, 3, 6, 9, 1, 4, 7, 10, 2, 5, 8, 11), brokerRacks, Collections.emptyList()));
    Collections.reverse(brokerList);
    assertEquals(brokerList,
        TenantPartitionAssignor.rackAlternatedBrokerList(brokerList, brokerRacks, Collections.emptyList()));
    for (int i = 0; i < 5; i++) {
      Collections.shuffle(brokerList);
      List<Integer> alternateList = TenantPartitionAssignor.rackAlternatedBrokerList(brokerList, brokerRacks, Collections.emptyList());
      List<Integer> rackOrder = alternateList.subList(0, numRacks).stream()
          .map(b -> b % numRacks).collect(Collectors.toList());

      // Verify that the list is rack-alternate
      List<List<Integer>> rackAssignment = new ArrayList<>();
      for (int j = 0; j < numRacks; j++) {
        rackAssignment.add(new ArrayList<>());
      }
      String errorMessage = "Unexpected assignment for " + brokerList + " : " + alternateList;
      for (int j = 0; j < numBrokers; j++) {
        int expectedRack = rackOrder.get(j % numRacks);
        int brokerId = alternateList.get(j);
        assertEquals(errorMessage, expectedRack, brokerId % numRacks);
        rackAssignment.get(expectedRack).add(brokerId);
      }

      // Verify that within a rack the list is ordered as in `brokerList`
      rackAssignment.forEach(list -> {
        int prevIndex = -1;
        for (int broker : list) {
          int curIndex = brokerList.indexOf(broker);
          assertTrue(errorMessage, curIndex > prevIndex);
          prevIndex = curIndex;
        }
      });

      // Verify that the racks are ordered as in `brokerList`
      List<Integer> expectedRackOrder = new ArrayList<>();
      for (int j = 0; j < numBrokers; j++) {
        int rack = brokerList.get(j) % numRacks;
        if (!expectedRackOrder.contains(rack)) {
          expectedRackOrder.add(rack);
          if (expectedRackOrder.size() == numRacks)
            break;
        }
      }
      assertEquals(errorMessage, expectedRackOrder, rackOrder);
    }
  }

  private void createTopic(String topic, int partitions, int replicationFactor) {
    String tenant = topic.substring(0, topic.indexOf("_"));
    TopicInfo topicInfo = new TopicInfo(partitions, (short) replicationFactor, 0);
    Map<String, TopicInfo> topicInfos = Collections.singletonMap(topic, topicInfo);
    Map<String, List<List<Integer>>> assignment = assignNewTopics(tenant, topicInfos);
    testCluster.createPartitions(topic, 0, assignment.get(topic));
  }

  private void addPartitions(String topic, int firstNewPartition, int newPartitions) {
    int totalPartitions = firstNewPartition + newPartitions;
    Map<String, Integer> partitions = Collections.singletonMap(topic, totalPartitions);
    Map<String, List<List<Integer>>> assignment = assignNewPartitions(partitions);
    testCluster.createPartitions(topic, firstNewPartition, assignment.get(topic));
  }

  private void addNodes(int count, int racks) {
    for (int i = 0; i < count; i++) {
      String rack = racks == 0 ? null : "rack" + (i % racks);
      testCluster.addNode(i, rack);
    }
    partitionAssignor.updateClusterMetadata(testCluster.cluster());
    if (racks > 0) {
      assertTrue(testCluster.rackAware());
    }
  }

  private Map<String, List<List<Integer>>> assignNewTopics(String tenant, Map<String, TopicInfo> topicInfos) {
    Map<String, List<List<Integer>>> assignments =
        partitionAssignor.assignPartitionsForNewTopics(tenant, topicInfos);
    assertEquals(topicInfos.size(), assignments.size());
    for (Map.Entry<String, TopicInfo> entry : topicInfos.entrySet()) {
      String topic = entry.getKey();
      TopicInfo topicInfo = entry.getValue();
      List<List<Integer>> assignment = assignments.get(topic);
      assertEquals(topicInfo.totalPartitions, assignment.size());
      testCluster.createPartitions(topic, 0, assignment);
    }
    partitionAssignor.updateClusterMetadata(testCluster.cluster());
    return assignments;
  }

  private Map<String, List<List<Integer>>> assignNewPartitions(Map<String, Integer> partitionInfos) {
    Map<String, List<List<Integer>>> assignments =
        partitionAssignor.assignPartitionsForExistingTopics("tenant1", partitionInfos);
    assertEquals(partitionInfos.size(), assignments.size());
    for (Map.Entry<String, Integer> entry : partitionInfos.entrySet()) {
      String topic = entry.getKey();
      int totalPartitions = entry.getValue();
      int firstNewPartition = testCluster.cluster().partitionsForTopic(topic).size();
      int newPartitions = totalPartitions - firstNewPartition;
      List<List<Integer>> assignment = assignments.get(topic);
      assertEquals(newPartitions, assignment.size());
      testCluster.createPartitions(topic, firstNewPartition, assignment);
    }
    partitionAssignor.updateClusterMetadata(testCluster.cluster());
    return assignments;
  }

  private void verifyAssignmentsAreBalanced(String tenant, Map<String, List<List<Integer>>> assignments,
                                            int maxReplicationFactor) {
    Map<Node, Integer> leaders = testCluster.partitionCountByNode(tenant, true);
    Map<Node, Integer> followers = testCluster.partitionCountByNode(tenant, false);
    for (List<List<Integer>> assignment : assignments.values()) {
      for (List<Integer> replicas : assignment) {
        for (int i  = 0; i < replicas.size(); i++) {
          Node node = testCluster.cluster().nodeById(replicas.get(i));
          if (i == 0) {
            leaders.put(node, leaders.get(node) + 1);
          } else {
            followers.put(node, leaders.get(node) + 1);
          }
        }
      }
    }
    int maxReplicaDiff = maxReplicationFactor - 1;
    if (testCluster.rackAware()) {
      maxReplicaDiff *= testCluster.racks().size();
    }
    verifyAssignmentsAreBalanced(tenant, maxReplicaDiff);
  }

  private void verifyAssignmentsAreBalanced(String tenant, int maxReplicaDiff) {
    Collection<Integer> leaders = testCluster.partitionCountByNode(tenant, true).values();
    Collection<Integer> followers = testCluster.partitionCountByNode(tenant, false).values();

    int maxLeaderImbalance = testCluster.rackAware() ? testCluster.racks().size() : 1;
    assertTrue("Leaders not balanced " + leaders,
        Collections.max(leaders) - Collections.min(leaders) <= maxLeaderImbalance);
    assertTrue("Follower replicas not balanced " + followers,
        Collections.max(followers) - Collections.min(followers) <= maxReplicaDiff);
  }

  private void verifyAssignmentsAreBalanced(int... maxDiff) {
    for (int i = 0; i < maxDiff.length; i++) {
      List<Integer> replicaCounts = followerCountsByNode(Optional.of(i));
      Collections.sort(replicaCounts);
      assertTrue("Replicas not balanced for replica #" + i + " : " + replicaCounts,
          replicaCounts.get(replicaCounts.size() - 1) - replicaCounts.get(0) <= maxDiff[i]);
    }
  }

  private List<Integer> leaderCountsByNode() {
    Cluster cluster = testCluster.cluster();
    return cluster.nodes().stream()
        .map(node -> cluster.partitionsForNode(node.id()).size())
        .collect(Collectors.toList());
  }

  private List<Integer> followerCountsByNode(Optional<Integer> replicaIndex) {
    Cluster cluster = testCluster.cluster();
    Map<Node, Integer> partitionCounts = cluster.nodes().stream()
        .collect(Collectors.toMap(Function.identity(), n -> 0));
    for (String topic : cluster.topics()) {
      for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
        Node[] replicas = partitionInfo.replicas();
        replicaIndex.ifPresent(r ->
            partitionCounts.put(replicas[r], partitionCounts.get(replicas[r]) + 1)
        );
        if (!replicaIndex.isPresent()) {
          for (int i = 1; i < replicas.length; i++) {
            partitionCounts.put(replicas[i], partitionCounts.get(replicas[i]) + 1);
          }
        }
      }
    }
    return new ArrayList<>(partitionCounts.values());
  }

  private void verifyPartitionCountsOnNodes(boolean leader, Integer... sortedPartitionCounts) {
    List<Integer> expectedPartitionCounts = Arrays.asList(sortedPartitionCounts);
    List<Integer> actualPartitionCounts = leader ? leaderCountsByNode()
        : followerCountsByNode(Optional.empty());
    Collections.sort(actualPartitionCounts);
    assertEquals(expectedPartitionCounts, actualPartitionCounts);
  }

  private void verifyAssignments() {
    Cluster cluster = testCluster.cluster();
    for (String topic : cluster.topics()) {
      for (PartitionInfo partitionInfo : cluster.partitionsForTopic(topic)) {
        Set<Integer> replicas = new HashSet<>();
        Set<String> replicaRacks = new HashSet<>();
        for (Node node : partitionInfo.replicas()) {
          replicas.add(node.id());
          if (node.rack() != null) {
            replicaRacks.add(node.rack());
          }
        }
        // Verify that replicas are distinct
        assertEquals(partitionInfo.replicas().length, replicas.size());
        if (testCluster.rackAware()) {
          assertEquals(partitionInfo.replicas().length, replicaRacks.size());
        }
      }
    }
  }

  private void verifyAssignment(List<List<Integer>> expectedAssignment, List<PartitionInfo> partitions) {
    List<List<Integer>> actualAssignment = new ArrayList<>(partitions.size());
    for (int i = 0; i < partitions.size(); i++) {
      actualAssignment.add(null);
    }
    partitions.forEach(partitionInfo ->
      actualAssignment.set(partitionInfo.partition(),
          Arrays.stream(partitionInfo.replicas()).map(Node::id).collect(Collectors.toList()))
    );
    assertEquals(expectedAssignment, actualAssignment);
  }
}
