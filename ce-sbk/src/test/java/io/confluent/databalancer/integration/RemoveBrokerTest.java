/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.integration;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import io.confluent.databalancer.KafkaDataBalanceManager;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidBrokerRemovalException;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.BalancerOfflineException;
import org.apache.kafka.common.errors.BalancerOperationFailedException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class RemoveBrokerTest extends DataBalancerClusterTestHarness {
  protected static final Logger log = LoggerFactory.getLogger(RemoveBrokerTest.class);

  @Rule
  final public Timeout globalTimeout = Timeout.millis(Duration.ofMinutes(5).toMillis());

  protected static Duration removalFinishTimeout = Duration.ofMinutes(3);
  protected static Duration removalPollInterval = Duration.ofSeconds(2);

  protected AtomicBoolean exited = new AtomicBoolean(false);

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    Exit.resetExitProcedure();
    exited.set(false);
  }

  @Override
  protected int initialBrokerCount() {
    return 3;
  }

  @Test
  public void testRemoveBroker_DisabledBalancerShouldThrowBalancerOfflineException() throws InterruptedException, ExecutionException {
    int brokerToRemoveId = notControllerKafkaServer().config().brokerId();

    // disable SBK
    ConfigResource cluster = new ConfigResource(ConfigResource.Type.BROKER, "");
    adminClient.incrementalAlterConfigs(Utils.mkMap(Utils.mkEntry(cluster,
        Collections.singleton(
            new AlterConfigOp(
                new ConfigEntry(ConfluentConfigs.BALANCER_ENABLE_CONFIG, "false"),
                AlterConfigOp.OpType.SET))))).all().get();

    // Await SBK shutdown
    KafkaServer controllerServer = controllerKafkaServer();
    KafkaDataBalanceManager dataBalancer = (KafkaDataBalanceManager) controllerServer.kafkaController().dataBalancer().get();
    org.apache.kafka.test.TestUtils.waitForCondition(() -> !dataBalancer.isActive(),
        15_000L,
        String.format("The databalancer did not start in %s", 15_000L)
    );

    // should throw BalancerOfflineException
    ExecutionException exception = assertThrows(ExecutionException.class, () -> adminClient.removeBrokers(Collections.singletonList(brokerToRemoveId)).all().get());
    assertNotNull("Expected to have a cause for the execution exception", exception.getCause());
    assertEquals(BalancerOfflineException.class, exception.getCause().getClass());
  }

  @Test
  public void testRemoveBroker() throws InterruptedException, ExecutionException {
    KafkaTestUtils.createTopic(adminClient, "test-topic", 20, 2);
    removeBroker(notControllerKafkaServer());
  }

  /**
   * If we are to move all replicas off the broker to be removed,
   * the removal should still complete successfully despite there being no goal proposals to execute
   */
  @Test
  public void testRemoveBroker_NoProposalsShouldComplete() throws InterruptedException, ExecutionException {
    KafkaServer server = notControllerKafkaServer();
    int brokerToRemoveId = server.config().brokerId();
    while (moveReplicasOffBroker(brokerToRemoveId).size() != 0) {
      info("Moving replicas off of broker {}", brokerToRemoveId);
    }
    removeBroker(notControllerKafkaServer());
  }

  @Test
  public void testRemoveController() throws InterruptedException, ExecutionException {
    KafkaTestUtils.createTopic(adminClient, "test-topic", 20, 2);
    removeBroker(controllerKafkaServer());
  }

  /**
   * Confirm that we can remove broker that is not alive. And while we have got a live cluster
   * setup, also validate input parameter checks.
   */
  @Test
  public void testDeadBroker() throws Exception {
    KafkaTestUtils.createTopic(adminClient, "test-topic", 20, 2);

    // Call remove with empty broker list
    InvalidBrokerRemovalException ex = null;
    try {
      adminClient.removeBrokers(Collections.emptyList()).all().get();
    } catch (ExecutionException e) {
      ex = (InvalidBrokerRemovalException) e.getCause();
    }
    assertNotNull("Able to remove broker with empty list.", ex);

    // Call remove with negative broker id
    ex = null;
    try {
      adminClient.removeBrokers(Collections.singletonList(-1)).all().get();
    } catch (ExecutionException e) {
      ex = (InvalidBrokerRemovalException) e.getCause();
    }
    assertNotNull("Able to remove broker with negative id.", ex);

    // Call remove with non existent broker id
    ex = null;
    try {
      adminClient.removeBrokers(Collections.singletonList(1_000)).all().get();
    } catch (ExecutionException e) {
      ex = (InvalidBrokerRemovalException) e.getCause();
    }
    assertNotNull("Able to remove non existent broker with id: 1000", ex);

    KafkaServer brokerToRemove = notControllerKafkaServer();
    brokerToRemove.shutdown();
    exited.set(true);

    removeBroker(brokerToRemove);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testRemoveBroker_FailureInShutdownShouldShowBalancerFailedOperation() throws ExecutionException, InterruptedException {
    KafkaServer controllerServer = controllerKafkaServer();
    KafkaServer toBeRemovedServer = notControllerKafkaServer();
    int brokerToRemoveId = toBeRemovedServer.config().brokerId();

    // remove the broker epoch, ensuring the shutdown request fails
    HashSet<Integer> brokersToRemove = new HashSet<>();
    brokersToRemove.add(brokerToRemoveId);
    controllerServer.kafkaController().controllerContext().removeLiveBrokers(JavaConverters.asScalaSet(brokersToRemove).toSet());

    // should throw BalancerOperationFailedException
    assertThrows(BalancerOperationFailedException.class, () -> removeBroker(toBeRemovedServer));
    Map<Integer, BrokerRemovalDescription> descriptionMap = adminClient.describeBrokerRemovals().descriptions().get();
    if (descriptionMap.isEmpty()) {
      fail("Expected to have broker removals to describe");
    }
    BrokerRemovalDescription brokerRemovalDescription = descriptionMap.get(brokerToRemoveId);
    assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED, brokerRemovalDescription.partitionReassignmentsStatus());
    assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.FAILED, brokerRemovalDescription.brokerShutdownStatus());
  }

  private void removeBroker(KafkaServer server) throws InterruptedException, ExecutionException {
    int brokerToRemoveId = server.config().brokerId();

    Exit.setExitProcedure((statusCode, message) -> {
      info("Shutting down {} as part of broker removal test", server.config().brokerId());
      server.shutdown();
      exited.set(true);
    });

    info("Removing broker with id {}", brokerToRemoveId);
    adminClient.removeBrokers(Collections.singletonList(brokerToRemoveId)).all().get();

    AtomicReference<ApiException> failException = new AtomicReference<>();
    // await removal completion and retry removal in case something went wrong
    TestUtils.waitForCondition(() -> {
          Map<Integer, BrokerRemovalDescription> descriptionMap = adminClient.describeBrokerRemovals().descriptions().get();
          if (descriptionMap.isEmpty()) {
            return false;
          }
          BrokerRemovalDescription brokerRemovalDescription = descriptionMap.get(brokerToRemoveId);

          if (isCompletedRemoval(brokerRemovalDescription)) {
            return true;
          } else if (isFailedPlanComputationInRemoval(brokerRemovalDescription)) {
            // a common failure is not having enough metrics for plan computation - simply retry it
            return retryRemoval(brokerRemovalDescription, brokerToRemoveId);
          } else if (isFailedRemoval(brokerRemovalDescription)) {
            String errMsg = String.format("Broker removal failed for an unexpected reason - description object %s", brokerRemovalDescription);
            failException.set((ApiException) brokerRemovalDescription.removalError().get().exception());
            info(errMsg);
            return true;
          } else {
            info("Removal is still pending. PAR: {} BSS: {}",
                brokerRemovalDescription.partitionReassignmentsStatus(), brokerRemovalDescription.brokerShutdownStatus());
            return false;
          }
        },
        removalFinishTimeout.toMillis(),
        removalPollInterval.toMillis(),
        () -> "Broker removal did not complete successfully in time!"
    );
    if (failException.get() != null) {
      throw failException.get();
    }

    assertTrue("Expected Exit to be called", exited.get());
    TestUtils.waitForCondition(() -> adminClient.describeCluster().nodes().get().size() == initialBrokerCount() - 1,
        60_000L, "Cluster size did not shrink!");
    assertEquals("Expected one broker removal to be stored in memory", 1,
            adminClient.describeBrokerRemovals().descriptions().get().size());
  }

  /**
   * Return all the brokers without the one which is getting removed
   */
  private List<Integer> brokerIdsWithoutRemovedBroker(int removedBrokerId) {
    List<Integer> brokers = servers.stream().map(server -> server.config().brokerId()).collect(Collectors.toList());
    brokers.remove(removedBrokerId);
    return brokers;
  }

  private List<TopicPartition> partitionsOnBroker(int brokerId) throws ExecutionException, InterruptedException {
    Set<String> topics = adminClient.listTopics().names().get();
    List<TopicPartition> topicPartitions = adminClient.describeTopics(topics).all().get().entrySet().stream()
        .flatMap(kv -> kv.getValue()
            .partitions().stream()
            .filter(tpInfo -> tpInfo.replicas().stream().map(Node::id).anyMatch(id -> id == brokerId))
            .map(tpInfo -> new TopicPartition(kv.getKey(), tpInfo.partition())))
        .collect(Collectors.toList());
    info("Partitions on broker {} are {}", brokerId, topicPartitions);
    return topicPartitions;
  }

  /**
   * Reassigns all the replicas away from the given broker
   * @return All the remaining, if any, replicas on broker #{@code brokerId}
   */
  private List<TopicPartition> moveReplicasOffBroker(int brokerId) throws ExecutionException, InterruptedException {
    List<Integer> brokerIds = brokerIdsWithoutRemovedBroker(brokerId);
    List<TopicPartition> topicPartitions = partitionsOnBroker(brokerId);
    adminClient.alterPartitionReassignments(topicPartitions.stream().collect(Collectors.toMap(
        tp -> tp,
        tp -> Optional.of(new NewPartitionReassignment(brokerIds))
    ))).all().get();
    TestUtils.waitForCondition(() -> adminClient.listPartitionReassignments().reassignments().get().size() == 0,
        60_000L,
        () -> "Expected all ongoing partition reassignments to finish");
    return partitionsOnBroker(brokerId);
  }
}
