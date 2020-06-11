package io.confluent.databalancer.integration;

import io.confluent.databalancer.KafkaDataBalanceManager;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import io.confluent.kafka.test.utils.KafkaTestUtils;
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
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class RemoveBrokerTest extends DataBalancerClusterTestHarness {
  protected static final Logger log = LoggerFactory.getLogger(RemoveBrokerTest.class);

  @Rule
  final public Timeout globalTimeout = Timeout.millis(Duration.ofMinutes(5).toMillis());

  protected static Duration removalFinishTimeout = Duration.ofMinutes(3);
  protected static Duration removalPollInterval = Duration.ofSeconds(2);

  protected AtomicBoolean exited = new AtomicBoolean(false);
  protected int brokerToRemoveId;
  protected KafkaServer serverToRemove;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    serverToRemove = notControllerKafkaServer();
    brokerToRemoveId = serverToRemove.config().brokerId();

    Exit.setExitProcedure((statusCode, message) -> {
      info("Shutting down {} as part of broker removal test", serverToRemove.config().brokerId());
      serverToRemove.shutdown();
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
    KafkaTestUtils.createTopic(adminClient, "test-topic", 20, 2);
    KafkaServer controllerServer = controllerKafkaServer();
    KafkaDataBalanceManager dataBalancer = (KafkaDataBalanceManager) controllerServer.kafkaController().dataBalancer().get();

    info("Removing broker with id {}", brokerToRemoveId);
    adminClient.removeBrokers(Collections.singletonList(brokerToRemoveId)).all().get();

    AtomicReference<String> failMsg = new AtomicReference<>();
    // await removal completion and retry removal in case something went wrong
    TestUtils.waitForCondition(() -> {
          Map<Integer, BrokerRemovalDescription> descriptionMap = adminClient.describeBrokerRemovals().descriptions().get();
          if (descriptionMap.isEmpty()) {
            return false;
          }
          BrokerRemovalDescription brokerRemovalDescription = descriptionMap.get(1);

          if (isCompletedRemoval(brokerRemovalDescription)) {
            return true;
          } else if (isFailedPlanComputationInRemoval(brokerRemovalDescription)) {
            // a common failure is not having enough metrics for plan computation - simply retry it
            return retryRemoval(brokerRemovalDescription, brokerToRemoveId);
          } else if (isFailedRemoval(brokerRemovalDescription)) {
            String errMsg = String.format("Broker removal failed for an unexpected reason - description object %s", brokerRemovalDescription);
            failMsg.set(errMsg);
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
    if (failMsg.get() != null && !failMsg.get().isEmpty()) {
      fail(failMsg.get());
    }

    assertTrue("Expected Exit to be called", exited.get());
    TestUtils.waitForCondition(() -> adminClient.describeCluster().nodes().get().size() == initialBrokerCount() - 1,
        60_000L, "Cluster size did not shrink!");
    assertEquals("Expected one broker removal to be stored in memory", 1, dataBalancer.brokerRemovals().size());
  }

}
