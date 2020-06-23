/*
 Copyright 2020 Confluent Inc.
 */
package io.confluent.databalancer.integration;

import io.confluent.kafka.test.utils.KafkaTestUtils;
import kafka.server.KafkaServer;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/*
 * This test validates that when a new broker arrives and the default configuration is set to
 * not self-heal (not set to ANY_UNEVEN_LOAD), the broker is still added.
 * The success criterion matches that of AddBrokerWithSelfHealing.
 */
@Category(IntegrationTest.class)
public class AddBrokerTest extends DataBalancerClusterTestHarness {
  private static final String TEST_TOPIC = "broker_addition_test_topic";

  @Rule
  final public Timeout globalTimeout = Timeout.millis(Duration.ofMinutes(3).toMillis());

  @Override
  protected int initialBrokerCount() {
    return 3;
  }

  @Override
  protected Properties injectTestSpecificProperties(Properties props) {
    return props;
  }

  @Test
  public void testBrokerAddition() throws Exception {
    KafkaTestUtils.createTopic(adminClient, TEST_TOPIC, 20, 2);

    int newBrokerId = initialBrokerCount() + 1;
    info("Adding new broker");
    addBroker(newBrokerId);
    DataBalancerIntegrationTestUtils.verifyReplicasMovedToBroker(adminClient, TEST_TOPIC, newBrokerId);
  }

  @Test
  public void testRemovedBrokerCanBeAdded() throws InterruptedException, ExecutionException {
    KafkaTestUtils.createTopic(adminClient, "test-topic", 20, 2);
    KafkaServer brokerToRemove = notControllerKafkaServer();

    AtomicBoolean exited = new AtomicBoolean(false);
    removeBroker(brokerToRemove, exited);
    int brokerIdToAdd = brokerToRemove.config().brokerId();
    addBroker(brokerIdToAdd);
    DataBalancerIntegrationTestUtils.verifyReplicasMovedToBroker(adminClient, "test-topic", brokerIdToAdd);
  }
}
