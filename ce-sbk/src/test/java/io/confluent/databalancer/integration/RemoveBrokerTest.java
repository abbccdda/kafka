package io.confluent.databalancer.integration;

import io.confluent.databalancer.KafkaDataBalanceManager;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class RemoveBrokerTest extends DataBalancerClusterTestHarness {
  protected static final Logger log = LoggerFactory.getLogger(RemoveBrokerTest.class);

  @Rule
  final public Timeout globalTimeout = Timeout.millis(Duration.ofMinutes(5).toMillis());

  private static Duration removalFinishTimeout = Duration.ofMinutes(3);
  private static Duration removalPollInterval = Duration.ofSeconds(2);

  private AtomicBoolean exited = new AtomicBoolean(false);
  private int brokerToRemoveId;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    KafkaServer server = notControllerKafkaServer();
    brokerToRemoveId = server.config().brokerId();

    Exit.setExitProcedure((statusCode, message) -> {
      info("Shutting down {} as part of broker removal test", server.config().brokerId());
      server.shutdown();
      exited.set(true);
    });
  }

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
  public void testRemoveBroker() throws InterruptedException, ExecutionException {
    KafkaServer controllerServer = controllerKafkaServer();
    KafkaDataBalanceManager dataBalancer = (KafkaDataBalanceManager) controllerServer.kafkaController().dataBalancer().get();

    TestUtils.waitForCondition(dataBalancer::isActive, "Waiting for DataBalancer to start.");

    info("Removing broker with id {}", brokerToRemoveId);
    adminClient.removeBrokers(Collections.singletonList(brokerToRemoveId)).all().get();

    // await removal completion and retry removal in case something went wrong
    TestUtils.waitForCondition(() -> {
          Map<Integer, BrokerRemovalDescription> descriptionMap = adminClient.describeBrokerRemovals().descriptions().get();
          if (descriptionMap.isEmpty()) {
            return false;
          }
          BrokerRemovalDescription brokerRemovalDescription = descriptionMap.get(1);

          if (isCompletedRemoval(brokerRemovalDescription)) {
            return true;
          } else if (isFailedRemoval(brokerRemovalDescription)) {
            // a common failure is not having enough metrics for plan computation - simply retry it
            info("Broker removal failed due to", brokerRemovalDescription.removalError().get().exception());
            info("Re-scheduling removal...");
            adminClient.removeBrokers(Collections.singletonList(brokerToRemoveId)).all().get();
            return false;
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

    assertTrue("Expected Exit to be called", exited.get());
    TestUtils.waitForCondition(() -> adminClient.describeCluster().nodes().get().size() == initialBrokerCount() - 1,
        60_000L, "Cluster size did not shrink!");
    assertEquals("Expected one broker removal to be stored in memory", 1, dataBalancer.brokerRemovals().size());
  }

  private boolean isCompletedRemoval(BrokerRemovalDescription brokerRemovalDescription) {
    boolean reassignmentCompleted = brokerRemovalDescription.partitionReassignmentsStatus() == BrokerRemovalDescription.PartitionReassignmentsStatus.COMPLETE;
    boolean removalCompleted = brokerRemovalDescription.brokerShutdownStatus() == BrokerRemovalDescription.BrokerShutdownStatus.COMPLETE;

    return removalCompleted && reassignmentCompleted;
  }

  private boolean isFailedRemoval(BrokerRemovalDescription brokerRemovalDescription) {
    boolean reassignmentFailed = brokerRemovalDescription.partitionReassignmentsStatus() == BrokerRemovalDescription.PartitionReassignmentsStatus.CANCELED
        || brokerRemovalDescription.partitionReassignmentsStatus() == BrokerRemovalDescription.PartitionReassignmentsStatus.FAILED;
    boolean removalFailed = brokerRemovalDescription.brokerShutdownStatus() == BrokerRemovalDescription.BrokerShutdownStatus.FAILED
        || brokerRemovalDescription.brokerShutdownStatus() == BrokerRemovalDescription.BrokerShutdownStatus.CANCELED;

    return removalFailed || reassignmentFailed;
  }
}
