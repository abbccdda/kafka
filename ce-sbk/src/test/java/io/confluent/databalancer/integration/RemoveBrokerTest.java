/*
 * Copyright (C) 2020 Confluent Inc.
 */
package io.confluent.databalancer.integration;

import io.confluent.databalancer.KafkaDataBalanceManager;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.BalancerOfflineException;
import org.apache.kafka.common.errors.BalancerOperationFailedException;
import org.apache.kafka.common.errors.InvalidBrokerRemovalException;
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

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class RemoveBrokerTest extends DataBalancerClusterTestHarness {
  protected static final Logger log = LoggerFactory.getLogger(RemoveBrokerTest.class);

  @Rule
  final public Timeout globalTimeout = Timeout.millis(Duration.ofMinutes(7).toMillis());

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
    removeBroker(notControllerKafkaServer(), exited);
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
    removeBroker(notControllerKafkaServer(), exited);
  }

  /**
   * Test failover handling of the broker removal operation by removing the Confluent Balancer server
   */
  @Test
  public void testRemoveController() throws InterruptedException, ExecutionException {
    KafkaTestUtils.createTopic(adminClient, "test-topic", 20, 2);
    removeBroker(controllerKafkaServer(), exited);
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

    removeBroker(brokerToRemove, exited);
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
    assertThrows(BalancerOperationFailedException.class, () -> removeBroker(toBeRemovedServer, exited));
    Map<Integer, BrokerRemovalDescription> descriptionMap = adminClient.describeBrokerRemovals().descriptions().get();
    if (descriptionMap.isEmpty()) {
      fail("Expected to have broker removals to describe");
    }
    BrokerRemovalDescription brokerRemovalDescription = descriptionMap.get(brokerToRemoveId);
    assertEquals(BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED, brokerRemovalDescription.partitionReassignmentsStatus());
    assertEquals(BrokerRemovalDescription.BrokerShutdownStatus.FAILED, brokerRemovalDescription.brokerShutdownStatus());
  }

  /**
   * Return all the brokers without the one which is getting removed
   */
  private List<Integer> brokerIdsWithoutRemovedBroker(int removedBrokerId) {
    List<Integer> brokers = servers.stream().map(server -> server.config().brokerId()).collect(Collectors.toList());
    brokers.remove(removedBrokerId);
    return brokers;
  }

  /**
   * Reassigns all the replicas away from the given broker
   * @return All the remaining, if any, replicas on broker #{@code brokerId}
   */
  private List<TopicPartition> moveReplicasOffBroker(int brokerId) throws ExecutionException, InterruptedException {
    List<Integer> brokerIds = brokerIdsWithoutRemovedBroker(brokerId);
    List<TopicPartition> topicPartitions = DataBalancerIntegrationTestUtils.partitionsOnBroker(brokerId, adminClient);
    adminClient.alterPartitionReassignments(topicPartitions.stream().collect(Collectors.toMap(
        tp -> tp,
        tp -> Optional.of(new NewPartitionReassignment(brokerIds))
    ))).all().get();
    TestUtils.waitForCondition(() -> adminClient.listPartitionReassignments().reassignments().get().size() == 0,
        60_000L,
        () -> "Expected all ongoing partition reassignments to finish");
    return DataBalancerIntegrationTestUtils.partitionsOnBroker(brokerId, adminClient);
  }
}
