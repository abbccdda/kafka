package io.confluent.databalancer.integration;

import io.confluent.databalancer.KafkaDataBalanceManager;
import io.confluent.kafka.test.utils.KafkaTestUtils;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.admin.BrokerRemovalDescription;
import org.apache.kafka.common.config.internals.ConfluentConfigs;
import org.apache.kafka.common.errors.BrokerRemovalCanceledException;
import org.apache.kafka.common.errors.BrokerRemovalInProgressException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

/**
 * An integration test for broker removal which sets the balancer throttle to a low value
 * in order to ensure the reassignments are slower
 */
@Category(IntegrationTest.class)
public class RemoveBrokerCancellationTest extends DataBalancerClusterTestHarness {
  protected static final Logger log = LoggerFactory.getLogger(RemoveBrokerCancellationTest.class);

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

  @Override
  protected Properties injectTestSpecificProperties(Properties props) {
    props.put(ConfluentConfigs.BALANCER_THROTTLE_CONFIG, 10L);
    return props;
  }

  @Test
  public void testRemoveBroker_TwoConsecutiveRemovalsResultInBrokerRemovalInProgressException() throws InterruptedException, ExecutionException {
    KafkaTestUtils.createTopic(adminClient, "test-topic", 50, 3);
    kafkaCluster.produceData("test-topic", 200);

    adminClient.removeBrokers(Collections.singletonList(brokerToRemoveId)).all().get();

    AtomicReference<String> failureMessage = new AtomicReference<>();
    // retry removal in case something went wrong
    // return the moment we see it's in progress
    // also await removal completion so that we have something to restart
    TestUtils.waitForCondition(() -> {
          Map<Integer, BrokerRemovalDescription> descriptionMap = adminClient.describeBrokerRemovals().descriptions().get();
          if (descriptionMap.isEmpty()) {
            return false;
          }
          BrokerRemovalDescription brokerRemovalDescription = descriptionMap.get(brokerToRemoveId);

          if (isFailedRemoval(brokerRemovalDescription)) {
            return retryRemoval(brokerRemovalDescription, brokerToRemoveId);
          } else if (isCompletedRemoval(brokerRemovalDescription)) {
            failureMessage.set("Broker removal finished successfully despite the broker restarting.");
            return true;
          } else if (isInProgressRemoval(brokerRemovalDescription)) {
              return true;
          } else {
            info("Removal is in an unknown state. PAR: {} BSS: {}",
                brokerRemovalDescription.partitionReassignmentsStatus(), brokerRemovalDescription.brokerShutdownStatus());
            return false;
          }
        },
        removalFinishTimeout.toMillis(),
        removalPollInterval.toMillis(),
        () -> "Broker removal did not become in progress in time!"
    );

    if (failureMessage.get() != null && !failureMessage.get().isEmpty()) {
      fail(failureMessage.get());
    }

    ExecutionException exc = assertThrows(ExecutionException.class,
        () -> adminClient.removeBrokers(Collections.singletonList(brokerToRemoveId)).all().get());
    assertNotNull("Expected exception to have a cause", exc.getCause());
    assertEquals(BrokerRemovalInProgressException.class, exc.getCause().getClass());
  }

  /**
   * Test the scenario where a broker removal operation is on-going and the broker comes back up (is restarted)
   */
  @Test
  public void testRemoveBroker_BrokerRestartCancelsRemoval() throws InterruptedException, ExecutionException {
    KafkaTestUtils.createTopic(adminClient, "test-topic", 50, 2);
    kafkaCluster.produceData("test-topic", 200);
    KafkaServer controllerServer = controllerKafkaServer();
    KafkaDataBalanceManager dataBalancer = (KafkaDataBalanceManager) controllerServer.kafkaController().dataBalancer().get();

    adminClient.removeBrokers(Collections.singletonList(brokerToRemoveId)).all().get();

    AtomicReference<String> failureMessage = new AtomicReference<>();

    // retry removal in case something went wrong
    // also await removal completion so that we have something to restart
    TestUtils.waitForCondition(() -> {
          Map<Integer, BrokerRemovalDescription> descriptionMap = adminClient.describeBrokerRemovals().descriptions().get();
          if (descriptionMap.isEmpty()) {
            return false;
          }
          BrokerRemovalDescription brokerRemovalDescription = descriptionMap.get(brokerToRemoveId);

          if (isFailedRemoval(brokerRemovalDescription)) {
            return retryRemoval(brokerRemovalDescription, brokerToRemoveId);
          } else if (exited.get()) {
            // restart the server to re-add the broker
            info("Broker {} was shut down due to removal. Restarting its server", brokerToRemoveId);
            serverToRemove.startup();
            return true;
          } else if (isCompletedRemoval(brokerRemovalDescription)) {
            failureMessage.set("Broker removal finished successfully despite the broker restarting.");
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

    if (failureMessage.get() != null && !failureMessage.get().isEmpty()) {
      fail(failureMessage.get());
    }

    assertEquals("Expected one broker removal to be stored in memory", 1, dataBalancer.brokerRemovals().size());
    TestUtils.waitForCondition(() -> dataBalancer.brokerRemovals().get(0).exception() instanceof BrokerRemovalCanceledException,
        60_000L,
        String.format("Broker removal status did not have the expected %s exception. Instead it has %s", BrokerRemovalCanceledException.class.getSimpleName(), dataBalancer.brokerRemovals().get(0).exception())
    );
  }
}
